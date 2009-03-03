/**
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
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Tests for the HStoreKey Plain and Meta RawComparators.
 */
public class TestHStoreKey extends TestCase {
  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testMoreComparisons() throws Exception {
    // Root compares
    HStoreKey a = new HStoreKey(".META.,,99999999999999");
    HStoreKey b = new HStoreKey(".META.,,1");
    HStoreKey.StoreKeyComparator c = new HStoreKey.RootStoreKeyComparator();
    assertTrue(c.compare(b.getBytes(), a.getBytes()) < 0);
    HStoreKey aa = new HStoreKey(".META.,,1");
    HStoreKey bb = new HStoreKey(".META.,,1", "info:regioninfo", 1235943454602L);
    assertTrue(c.compare(aa.getBytes(), bb.getBytes()) < 0);
    
    // Meta compares
    HStoreKey aaa = new HStoreKey("TestScanMultipleVersions,row_0500,1236020145502");
    HStoreKey bbb = new HStoreKey("TestScanMultipleVersions,,99999999999999");
    c = new HStoreKey.MetaStoreKeyComparator();
    assertTrue(c.compare(bbb.getBytes(), aaa.getBytes()) < 0);
    
    HStoreKey aaaa = new HStoreKey("TestScanMultipleVersions,,1236023996656",
      "info:regioninfo", 1236024396271L);
    assertTrue(c.compare(aaaa.getBytes(), bbb.getBytes()) < 0);
    
    HStoreKey x = new HStoreKey("TestScanMultipleVersions,row_0500,1236034574162",
      "", 9223372036854775807L);
    HStoreKey y = new HStoreKey("TestScanMultipleVersions,row_0500,1236034574162",
      "info:regioninfo", 1236034574912L);
    assertTrue(c.compare(x.getBytes(), y.getBytes()) < 0);
    
    comparisons(new HStoreKey.HStoreKeyRootComparator());
    comparisons(new HStoreKey.HStoreKeyMetaComparator());
    comparisons(new HStoreKey.HStoreKeyComparator());
    metacomparisons(new HStoreKey.HStoreKeyRootComparator());
    metacomparisons(new HStoreKey.HStoreKeyMetaComparator());
  }

  /**
   * Tests cases where rows keys have characters below the ','.
   * See HBASE-832
   * @throws IOException 
   */
  public void testHStoreKeyBorderCases() throws IOException {
    HStoreKey rowA = new HStoreKey("testtable,www.hbase.org/,1234",
      "", Long.MAX_VALUE);
    byte [] rowABytes = Writables.getBytes(rowA);
    HStoreKey rowB = new HStoreKey("testtable,www.hbase.org/%20,99999",
      "", Long.MAX_VALUE);
    byte [] rowBBytes = Writables.getBytes(rowB);
    // This is a plain compare on the row. It gives wrong answer for meta table
    // row entry.
    assertTrue(rowA.compareTo(rowB) > 0);
    HStoreKey.MetaStoreKeyComparator c =
      new HStoreKey.MetaStoreKeyComparator();
    assertTrue(c.compare(rowABytes, rowBBytes) < 0);

    rowA = new HStoreKey("testtable,,1234", "", Long.MAX_VALUE);
    rowB = new HStoreKey("testtable,$www.hbase.org/,99999", "", Long.MAX_VALUE);
    assertTrue(rowA.compareTo(rowB) > 0);
    assertTrue(c.compare( Writables.getBytes(rowA),  Writables.getBytes(rowB)) < 0);

    rowA = new HStoreKey(".META.,testtable,www.hbase.org/,1234,4321", "",
      Long.MAX_VALUE);
    rowB = new HStoreKey(".META.,testtable,www.hbase.org/%20,99999,99999", "",
      Long.MAX_VALUE);
    assertTrue(rowA.compareTo(rowB) > 0);
    HStoreKey.RootStoreKeyComparator rootComparator =
      new HStoreKey.RootStoreKeyComparator();
    assertTrue(rootComparator.compare( Writables.getBytes(rowA),
      Writables.getBytes(rowB)) < 0);
  }

  private void metacomparisons(final HStoreKey.HStoreKeyComparator c) {
    assertTrue(c.compare(new HStoreKey(".META.,a,,0,1"),
      new HStoreKey(".META.,a,,0,1")) == 0);
    assertTrue(c.compare(new HStoreKey(".META.,a,,0,1"),
      new HStoreKey(".META.,a,,0,2")) < 0);
    assertTrue(c.compare(new HStoreKey(".META.,a,,0,2"),
      new HStoreKey(".META.,a,,0,1")) > 0);
  }

  private void comparisons(final HStoreKey.HStoreKeyComparator c) {
    assertTrue(c.compare(new HStoreKey(".META.,,1"),
      new HStoreKey(".META.,,1")) == 0);
    assertTrue(c.compare(new HStoreKey(".META.,,1"),
      new HStoreKey(".META.,,2")) < 0);
    assertTrue(c.compare(new HStoreKey(".META.,,2"),
      new HStoreKey(".META.,,1")) > 0);
  }

  @SuppressWarnings("unchecked")
  public void testBinaryKeys() throws Exception {
	Set<HStoreKey> set = new TreeSet<HStoreKey>(new HStoreKey.HStoreKeyComparator());
	HStoreKey [] keys = {new HStoreKey("aaaaa,\u0000\u0000,2", getName(), 2),
	  new HStoreKey("aaaaa,\u0001,3", getName(), 3),
	  new HStoreKey("aaaaa,,1", getName(), 1),
	  new HStoreKey("aaaaa,\u1000,5", getName(), 5),
	  new HStoreKey("aaaaa,a,4", getName(), 4),
    new HStoreKey("a,a,0", getName(), 0),
	};
	// Add to set with bad comparator
	for (int i = 0; i < keys.length; i++) {
	  set.add(keys[i]);
	}
	// This will output the keys incorrectly.
	boolean assertion = false;
	int count = 0;
	try {
      for (HStoreKey k: set) {
        assertTrue(count++ == k.getTimestamp());
      }
	} catch (junit.framework.AssertionFailedError e) {
	  // Expected
	  assertion = true;
	}
	assertTrue(assertion);
	// Make set with good comparator
	set = new TreeSet<HStoreKey>(new HStoreKey.HStoreKeyMetaComparator());
	for (int i = 0; i < keys.length; i++) {
      set.add(keys[i]);
    }
    count = 0;
    for (HStoreKey k: set) {
      assertTrue(count++ == k.getTimestamp());
    }
    // Make up -ROOT- table keys.
    HStoreKey [] rootKeys = {
        new HStoreKey(".META.,aaaaa,\u0000\u0000,0,2", getName(), 2),
        new HStoreKey(".META.,aaaaa,\u0001,0,3", getName(), 3),
        new HStoreKey(".META.,aaaaa,,0,1", getName(), 1),
        new HStoreKey(".META.,aaaaa,\u1000,0,5", getName(), 5),
        new HStoreKey(".META.,aaaaa,a,0,4", getName(), 4),
        new HStoreKey(".META.,,0", getName(), 0),
      };
    // This will output the keys incorrectly.
    set = new TreeSet<HStoreKey>(new HStoreKey.HStoreKeyMetaComparator());
    // Add to set with bad comparator
    for (int i = 0; i < keys.length; i++) {
      set.add(rootKeys[i]);
    }
    assertion = false;
    count = 0;
    try {
      for (HStoreKey k: set) {
        assertTrue(count++ == k.getTimestamp());
      }
    } catch (junit.framework.AssertionFailedError e) {
      // Expected
      assertion = true;
    }
    // Now with right comparator
    set = new TreeSet<HStoreKey>(new HStoreKey.HStoreKeyRootComparator());
    // Add to set with bad comparator
    for (int i = 0; i < keys.length; i++) {
      set.add(rootKeys[i]);
    }
    count = 0;
    for (HStoreKey k: set) {
      assertTrue(count++ == k.getTimestamp());
    }
  }

  public void testSerialization() throws IOException {
    HStoreKey hsk = new HStoreKey(getName(), getName(), 123);
    byte [] b = hsk.getBytes();
    HStoreKey hsk2 = HStoreKey.create(b);
    assertTrue(hsk.equals(hsk2));
    // Test getBytes with empty column
    hsk = new HStoreKey(getName());
    assertTrue(Bytes.equals(hsk.getBytes(),
      HStoreKey.getBytes(Bytes.toBytes(getName()), null,
      HConstants.LATEST_TIMESTAMP)));
  }

  public void testGetBytes() throws IOException {
    long now = System.currentTimeMillis();
    HStoreKey hsk = new HStoreKey("one", "two", now);
    byte [] writablesBytes = Writables.getBytes(hsk);
    byte [] selfSerializationBytes = hsk.getBytes();
    Bytes.equals(writablesBytes, selfSerializationBytes);
  }

  public void testByteBuffer() throws Exception {
    final long ts = 123;
    final byte [] row = Bytes.toBytes("row");
    final byte [] column = Bytes.toBytes("column");
    HStoreKey hsk = new HStoreKey(row, column, ts);
    ByteBuffer bb = ByteBuffer.wrap(hsk.getBytes());
    assertTrue(Bytes.equals(row, HStoreKey.getRow(bb)));
    assertTrue(Bytes.equals(column, HStoreKey.getColumn(bb)));
    assertEquals(ts, HStoreKey.getTimestamp(bb));
  }

  /**
   * Test the byte comparator works same as the object comparator.
   */
  public void testRawComparator() throws IOException {
    long timestamp = System.currentTimeMillis();
    byte [] a = Bytes.toBytes("a");
    HStoreKey past = new HStoreKey(a, a, timestamp - 10);
    byte [] pastBytes = Writables.getBytes(past);
    HStoreKey now = new HStoreKey(a, a, timestamp);
    byte [] nowBytes = Writables.getBytes(now);
    HStoreKey future = new HStoreKey(a, a, timestamp + 10);
    byte [] futureBytes = Writables.getBytes(future);
    HStoreKey.StoreKeyComparator comparator =
      new HStoreKey.StoreKeyComparator();
    assertTrue(past.compareTo(now) > 0);
    assertTrue(comparator.compare(pastBytes, nowBytes) > 0);
    assertTrue(now.compareTo(now) == 0);
    assertTrue(comparator.compare(nowBytes, nowBytes) == 0);
    assertTrue(future.compareTo(now) < 0);
    assertTrue(comparator.compare(futureBytes, nowBytes) < 0);
    // Check that empty column comes before one with a column
    HStoreKey nocolumn = new HStoreKey(a, timestamp);
    byte [] nocolumnBytes = Writables.getBytes(nocolumn);
    HStoreKey withcolumn = new HStoreKey(a, a, timestamp);
    byte [] withcolumnBytes = Writables.getBytes(withcolumn);
    assertTrue(nocolumn.compareTo(withcolumn) < 0);
    assertTrue(comparator.compare(nocolumnBytes, withcolumnBytes) < 0);
    // Check that empty column comes and LATEST comes before one with a column
    // and old timestamp.
    nocolumn = new HStoreKey(a, HConstants.LATEST_TIMESTAMP);
    nocolumnBytes = Writables.getBytes(nocolumn);
    withcolumn = new HStoreKey(a, a, timestamp);
    withcolumnBytes = Writables.getBytes(withcolumn);
    assertTrue(nocolumn.compareTo(withcolumn) < 0);
    assertTrue(comparator.compare(nocolumnBytes, withcolumnBytes) < 0);
  }
}