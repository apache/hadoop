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
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.MetaComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;

public class TestKeyValue extends TestCase {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());

  public void testColumnCompare() throws Exception {
    final byte [] a = Bytes.toBytes("aaa");
    byte [] family1 = Bytes.toBytes("abc");
    byte [] qualifier1 = Bytes.toBytes("def");
    byte [] family2 = Bytes.toBytes("abcd");
    byte [] qualifier2 = Bytes.toBytes("ef");

    KeyValue aaa = new KeyValue(a, family1, qualifier1, 0L, Type.Put, a);
    assertFalse(aaa.matchingColumn(family2, qualifier2));
    assertTrue(aaa.matchingColumn(family1, qualifier1));
    aaa = new KeyValue(a, family2, qualifier2, 0L, Type.Put, a);
    assertFalse(aaa.matchingColumn(family1, qualifier1));
    assertTrue(aaa.matchingColumn(family2,qualifier2));
    byte [] nullQualifier = new byte[0];
    aaa = new KeyValue(a, family1, nullQualifier, 0L, Type.Put, a);
    assertTrue(aaa.matchingColumn(family1,null));
    assertFalse(aaa.matchingColumn(family2,qualifier2));
  }

  public void testBasics() throws Exception {
    LOG.info("LOWKEY: " + KeyValue.LOWESTKEY.toString());
    check(Bytes.toBytes(getName()),
      Bytes.toBytes(getName()), Bytes.toBytes(getName()), 1,
      Bytes.toBytes(getName()));
    // Test empty value and empty column -- both should work. (not empty fam)
    check(Bytes.toBytes(getName()), Bytes.toBytes(getName()), null, 1, null);
    check(HConstants.EMPTY_BYTE_ARRAY, Bytes.toBytes(getName()), null, 1, null);
  }

  private void check(final byte [] row, final byte [] family, byte [] qualifier,
    final long timestamp, final byte [] value) {
    KeyValue kv = new KeyValue(row, family, qualifier, timestamp, value);
    assertTrue(Bytes.compareTo(kv.getRow(), row) == 0);
    assertTrue(kv.matchingColumn(family, qualifier));
    // Call toString to make sure it works.
    LOG.info(kv.toString());
  }

  public void testPlainCompare() throws Exception {
    final byte [] a = Bytes.toBytes("aaa");
    final byte [] b = Bytes.toBytes("bbb");
    final byte [] fam = Bytes.toBytes("col");
    final byte [] qf = Bytes.toBytes("umn");
//    final byte [] column = Bytes.toBytes("col:umn");
    KeyValue aaa = new KeyValue(a, fam, qf, a);
    KeyValue bbb = new KeyValue(b, fam, qf, b);
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
    aaa = new KeyValue(a, fam, qf, 1, a);
    bbb = new KeyValue(a, fam, qf, 2, a);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, bbb) > 0);
    assertTrue(KeyValue.COMPARATOR.compare(bbb, aaa) < 0);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, aaa) == 0);
    // Do compare with different types.  Higher numbered types -- Delete
    // should sort ahead of lower numbers; i.e. Put
    aaa = new KeyValue(a, fam, qf, 1, KeyValue.Type.Delete, a);
    bbb = new KeyValue(a, fam, qf, 1, a);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, bbb) < 0);
    assertTrue(KeyValue.COMPARATOR.compare(bbb, aaa) > 0);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, aaa) == 0);
  }

  public void testMoreComparisons() throws Exception {
    // Root compares
    long now = System.currentTimeMillis();
    KeyValue a = new KeyValue(Bytes.toBytes(".META.,,99999999999999"), now);
    KeyValue b = new KeyValue(Bytes.toBytes(".META.,,1"), now);
    KVComparator c = new KeyValue.RootComparator();
    assertTrue(c.compare(b, a) < 0);
    KeyValue aa = new KeyValue(Bytes.toBytes(".META.,,1"), now);
    KeyValue bb = new KeyValue(Bytes.toBytes(".META.,,1"),
        Bytes.toBytes("info"), Bytes.toBytes("regioninfo"), 1235943454602L,
        (byte[])null);
    assertTrue(c.compare(aa, bb) < 0);

    // Meta compares
    KeyValue aaa = new KeyValue(
        Bytes.toBytes("TestScanMultipleVersions,row_0500,1236020145502"), now);
    KeyValue bbb = new KeyValue(
        Bytes.toBytes("TestScanMultipleVersions,,99999999999999"), now);
    c = new KeyValue.MetaComparator();
    assertTrue(c.compare(bbb, aaa) < 0);

    KeyValue aaaa = new KeyValue(Bytes.toBytes("TestScanMultipleVersions,,1236023996656"),
        Bytes.toBytes("info"), Bytes.toBytes("regioninfo"), 1236024396271L,
        (byte[])null);
    assertTrue(c.compare(aaaa, bbb) < 0);

    KeyValue x = new KeyValue(Bytes.toBytes("TestScanMultipleVersions,row_0500,1236034574162"),
        Bytes.toBytes("info"), Bytes.toBytes(""), 9223372036854775807L,
        (byte[])null);
    KeyValue y = new KeyValue(Bytes.toBytes("TestScanMultipleVersions,row_0500,1236034574162"),
        Bytes.toBytes("info"), Bytes.toBytes("regioninfo"), 1236034574912L,
        (byte[])null);
    assertTrue(c.compare(x, y) < 0);
    comparisons(new KeyValue.MetaComparator());
    comparisons(new KeyValue.KVComparator());
    metacomparisons(new KeyValue.RootComparator());
    metacomparisons(new KeyValue.MetaComparator());
  }

  public void testBadMetaCompareSingleDelim() {
    MetaComparator c = new KeyValue.MetaComparator();
    long now = System.currentTimeMillis();
    // meta keys values are not quite right.  A users can enter illegal values 
    // from shell when scanning meta.
    KeyValue a = new KeyValue(Bytes.toBytes("table,a1"), now);
    KeyValue b = new KeyValue(Bytes.toBytes("table,a2"), now);
    try {
      c.compare(a, b);
    } catch (IllegalArgumentException iae) { 
      assertEquals(".META. key must have two ',' delimiters and have the following" +
      		" format: '<table>,<key>,<etc>'", iae.getMessage());
      return;
    }
    fail("Expected IllegalArgumentException");
  }

  public void testMetaComparatorTableKeysWithCommaOk() {
    MetaComparator c = new KeyValue.MetaComparator();
    long now = System.currentTimeMillis();
    // meta keys values are not quite right.  A users can enter illegal values 
    // from shell when scanning meta.
    KeyValue a = new KeyValue(Bytes.toBytes("table,key,with,commas1,1234"), now);
    KeyValue b = new KeyValue(Bytes.toBytes("table,key,with,commas2,0123"), now);
    assertTrue(c.compare(a, b) < 0);
  }
  
  /**
   * Tests cases where rows keys have characters below the ','.
   * See HBASE-832
   * @throws IOException
   */
  public void testKeyValueBorderCases() throws IOException {
    // % sorts before , so if we don't do special comparator, rowB would
    // come before rowA.
    KeyValue rowA = new KeyValue(Bytes.toBytes("testtable,www.hbase.org/,1234"),
      Bytes.toBytes("fam"), Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    KeyValue rowB = new KeyValue(Bytes.toBytes("testtable,www.hbase.org/%20,99999"),
        Bytes.toBytes("fam"), Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    assertTrue(KeyValue.META_COMPARATOR.compare(rowA, rowB) < 0);

    rowA = new KeyValue(Bytes.toBytes("testtable,,1234"), Bytes.toBytes("fam"),
        Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    rowB = new KeyValue(Bytes.toBytes("testtable,$www.hbase.org/,99999"),
        Bytes.toBytes("fam"), Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    assertTrue(KeyValue.META_COMPARATOR.compare(rowA, rowB) < 0);

    rowA = new KeyValue(Bytes.toBytes(".META.,testtable,www.hbase.org/,1234,4321"),
        Bytes.toBytes("fam"), Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    rowB = new KeyValue(Bytes.toBytes(".META.,testtable,www.hbase.org/%20,99999,99999"),
        Bytes.toBytes("fam"), Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    assertTrue(KeyValue.ROOT_COMPARATOR.compare(rowA, rowB) < 0);
  }

  private void metacomparisons(final KeyValue.MetaComparator c) {
    long now = System.currentTimeMillis();
    assertTrue(c.compare(new KeyValue(Bytes.toBytes(".META.,a,,0,1"), now),
      new KeyValue(Bytes.toBytes(".META.,a,,0,1"), now)) == 0);
    KeyValue a = new KeyValue(Bytes.toBytes(".META.,a,,0,1"), now);
    KeyValue b = new KeyValue(Bytes.toBytes(".META.,a,,0,2"), now);
    assertTrue(c.compare(a, b) < 0);
    assertTrue(c.compare(new KeyValue(Bytes.toBytes(".META.,a,,0,2"), now),
      new KeyValue(Bytes.toBytes(".META.,a,,0,1"), now)) > 0);
  }

  private void comparisons(final KeyValue.KVComparator c) {
    long now = System.currentTimeMillis();
    assertTrue(c.compare(new KeyValue(Bytes.toBytes(".META.,,1"), now),
      new KeyValue(Bytes.toBytes(".META.,,1"), now)) == 0);
    assertTrue(c.compare(new KeyValue(Bytes.toBytes(".META.,,1"), now),
      new KeyValue(Bytes.toBytes(".META.,,2"), now)) < 0);
    assertTrue(c.compare(new KeyValue(Bytes.toBytes(".META.,,2"), now),
      new KeyValue(Bytes.toBytes(".META.,,1"), now)) > 0);
  }

  public void testBinaryKeys() throws Exception {
    Set<KeyValue> set = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
    final byte [] fam = Bytes.toBytes("col");
    final byte [] qf = Bytes.toBytes("umn");
    final byte [] nb = new byte[0];
    KeyValue [] keys = {new KeyValue(Bytes.toBytes("aaaaa,\u0000\u0000,2"), fam, qf, 2, nb),
      new KeyValue(Bytes.toBytes("aaaaa,\u0001,3"), fam, qf, 3, nb),
      new KeyValue(Bytes.toBytes("aaaaa,,1"), fam, qf, 1, nb),
      new KeyValue(Bytes.toBytes("aaaaa,\u1000,5"), fam, qf, 5, nb),
      new KeyValue(Bytes.toBytes("aaaaa,a,4"), fam, qf, 4, nb),
      new KeyValue(Bytes.toBytes("a,a,0"), fam, qf, 0, nb),
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
        new KeyValue(Bytes.toBytes(".META.,aaaaa,\u0000\u0000,0,2"), fam, qf, 2, nb),
        new KeyValue(Bytes.toBytes(".META.,aaaaa,\u0001,0,3"), fam, qf, 3, nb),
        new KeyValue(Bytes.toBytes(".META.,aaaaa,,0,1"), fam, qf, 1, nb),
        new KeyValue(Bytes.toBytes(".META.,aaaaa,\u1000,0,5"), fam, qf, 5, nb),
        new KeyValue(Bytes.toBytes(".META.,aaaaa,a,0,4"), fam, qf, 4, nb),
        new KeyValue(Bytes.toBytes(".META.,,0"), fam, qf, 0, nb),
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

  public void testStackedUpKeyValue() {
    // Test multiple KeyValues in a single blob.

    // TODO actually write this test!

  }

  private final byte[] rowA = Bytes.toBytes("rowA");
  private final byte[] rowB = Bytes.toBytes("rowB");

  private final byte[] family = Bytes.toBytes("family");
  private final byte[] qualA = Bytes.toBytes("qfA");

  private void assertKVLess(KeyValue.KVComparator c,
                            KeyValue less,
                            KeyValue greater) {
    int cmp = c.compare(less,greater);
    assertTrue(cmp < 0);
    cmp = c.compare(greater,less);
    assertTrue(cmp > 0);
  }

  public void testFirstLastOnRow() {
    final KVComparator c = KeyValue.COMPARATOR;
    long ts = 1;

    // These are listed in sort order (ie: every one should be less
    // than the one on the next line).
    final KeyValue firstOnRowA = KeyValue.createFirstOnRow(rowA);
    final KeyValue kvA_1 = new KeyValue(rowA, null, null, ts, Type.Put);
    final KeyValue kvA_2 = new KeyValue(rowA, family, qualA, ts, Type.Put);
        
    final KeyValue lastOnRowA = KeyValue.createLastOnRow(rowA);
    final KeyValue firstOnRowB = KeyValue.createFirstOnRow(rowB);
    final KeyValue kvB = new KeyValue(rowB, family, qualA, ts, Type.Put);

    assertKVLess(c, firstOnRowA, firstOnRowB);
    assertKVLess(c, firstOnRowA, kvA_1);
    assertKVLess(c, firstOnRowA, kvA_2);
    assertKVLess(c, kvA_1, kvA_2);
    assertKVLess(c, kvA_2, firstOnRowB);
    assertKVLess(c, kvA_1, firstOnRowB);

    assertKVLess(c, lastOnRowA, firstOnRowB);
    assertKVLess(c, firstOnRowB, kvB);
    assertKVLess(c, lastOnRowA, kvB);

    assertKVLess(c, kvA_2, lastOnRowA);
    assertKVLess(c, kvA_1, lastOnRowA);
    assertKVLess(c, firstOnRowA, lastOnRowA);
  }

  public void testConvertToKeyOnly() throws Exception {
    long ts = 1;
    byte [] value = Bytes.toBytes("a real value");
    byte [] evalue = new byte[0]; // empty value

    for (byte[] val : new byte[][]{value, evalue}) {
      for (boolean useLen : new boolean[]{false,true}) {
        KeyValue kv1 = new KeyValue(rowA, family, qualA, ts, val);
        KeyValue kv1ko = kv1.clone();
        assertTrue(kv1.equals(kv1ko));
        kv1ko.convertToKeyOnly(useLen);
        // keys are still the same
        assertTrue(kv1.equals(kv1ko));
        // but values are not
        assertTrue(kv1ko.getValue().length == (useLen?Bytes.SIZEOF_INT:0));
        if (useLen) {
          assertEquals(kv1.getValueLength(), Bytes.toInt(kv1ko.getValue()));
        }
      }
    }
  }
}
