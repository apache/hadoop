/*
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

package org.apache.hadoop.hbase.regionserver;

import junit.framework.TestCase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;


public class TestStoreScanner extends TestCase {

  final byte [] CF = Bytes.toBytes("cf");
  
  /**
   * Test utility for building a NavigableSet for scanners.
   * @param strCols
   * @return
   */
  NavigableSet<byte[]> getCols(String ...strCols) {
    NavigableSet<byte[]> cols = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (String col : strCols) {
      byte[] bytes = Bytes.toBytes(col);
      cols.add(bytes);
    }
    return cols;
  }

  /**
   * Test test shows exactly how the matcher's return codes confuses the StoreScanner
   * and prevent it from doing the right thing.  Seeking once, then nexting twice
   * should return R1, then R2, but in this case it doesnt.
   * @throws IOException
   */
  public void testWontNextToNext() throws IOException {
    // build the scan file:
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 2, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", 1, KeyValue.Type.Put, "dont-care")
    };
    KeyValueScanner [] scanners = new KeyValueScanner[] {
        new KeyValueScanFixture(KeyValue.COMPARATOR,
            kvs)
    };

    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    // this only uses maxVersions (default=1) and TimeRange (default=all)
    StoreScanner scan =
        new StoreScanner(scanSpec, CF, Long.MAX_VALUE,
            KeyValue.COMPARATOR, getCols("a"),
            scanners);

    List<KeyValue> results = new ArrayList<KeyValue>();
    scan.next(results);
    assertEquals(1, results.size());
    assertEquals(kvs[0], results.get(0));
    // should be ok...
    // now scan _next_ again.
    results.clear();
    scan.next(results);
    assertEquals(1, results.size());
    assertEquals(kvs[2], results.get(0));

    results.clear();
    scan.next(results);
    assertEquals(0, results.size());

  }


  public void testDeleteVersionSameTimestamp() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Delete, "dont-care"),
    };
    KeyValueScanner [] scanners = new KeyValueScanner[] {
        new KeyValueScanFixture(KeyValue.COMPARATOR, kvs)
    };
    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    StoreScanner scan =
        new StoreScanner(scanSpec, CF, Long.MAX_VALUE, KeyValue.COMPARATOR,
            getCols("a"), scanners);

    List<KeyValue> results = new ArrayList<KeyValue>();
    assertFalse(scan.next(results));
    assertEquals(0, results.size());
  }

  /**
   * Test the case where there is a delete row 'in front of' the next row, the scanner
   * will move to the next row.
   */
  public void testDeletedRowThenGoodRow() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Delete, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", 20, KeyValue.Type.Put, "dont-care")
    };
    KeyValueScanner [] scanners = new KeyValueScanner[] {
        new KeyValueScanFixture(KeyValue.COMPARATOR, kvs)
    };
    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    StoreScanner scan =
        new StoreScanner(scanSpec, CF, Long.MAX_VALUE, KeyValue.COMPARATOR,
            getCols("a"), scanners);
    
    List<KeyValue> results = new ArrayList<KeyValue>();
    assertEquals(true, scan.next(results));
    assertEquals(0, results.size());

    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[2], results.get(0));

    assertEquals(false, scan.next(results));
  }

  public void testDeleteVersionMaskingMultiplePuts() throws IOException {
    long now = System.currentTimeMillis();
    KeyValue [] kvs1 = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Delete, "dont-care")
    };
    KeyValue [] kvs2 = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now-500, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now-100, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care")
    };
    KeyValueScanner [] scanners = new KeyValueScanner[] {
        new KeyValueScanFixture(KeyValue.COMPARATOR, kvs1),
        new KeyValueScanFixture(KeyValue.COMPARATOR, kvs2)
    };
    StoreScanner scan =
        new StoreScanner(new Scan(Bytes.toBytes("R1")), CF, Long.MAX_VALUE, KeyValue.COMPARATOR,
            getCols("a"), scanners);
    List<KeyValue> results = new ArrayList<KeyValue>();
    // the two put at ts=now will be masked by the 1 delete, and
    // since the scan default returns 1 version we'll return the newest
    // key, which is kvs[2], now-100.
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs2[1], results.get(0));
  }
  public void testDeleteVersionsMixedAndMultipleVersionReturn() throws IOException {
    long now = System.currentTimeMillis();
    KeyValue [] kvs1 = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Delete, "dont-care")
    };
    KeyValue [] kvs2 = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now-500, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now+500, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "z", now, KeyValue.Type.Put, "dont-care")
    };
    KeyValueScanner [] scanners = new KeyValueScanner[] {
        new KeyValueScanFixture(KeyValue.COMPARATOR, kvs1),
        new KeyValueScanFixture(KeyValue.COMPARATOR, kvs2)
    };
    Scan scanSpec = new Scan(Bytes.toBytes("R1")).setMaxVersions(2);
    StoreScanner scan =
        new StoreScanner(scanSpec, CF, Long.MAX_VALUE, KeyValue.COMPARATOR,
            getCols("a"), scanners);
    List<KeyValue> results = new ArrayList<KeyValue>();
    assertEquals(true, scan.next(results));
    assertEquals(2, results.size());
    assertEquals(kvs2[1], results.get(0));
    assertEquals(kvs2[0], results.get(1));
  }

  public void testWildCardOneVersionScan() throws IOException {
   KeyValue [] kvs = new KeyValue [] {
       KeyValueTestUtil.create("R1", "cf", "a", 2, KeyValue.Type.Put, "dont-care"),
       KeyValueTestUtil.create("R1", "cf", "b", 1, KeyValue.Type.Put, "dont-care"),
       KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.DeleteColumn, "dont-care"),
   };
    KeyValueScanner [] scanners = new KeyValueScanner[] {
        new KeyValueScanFixture(KeyValue.COMPARATOR, kvs)
    };
    StoreScanner scan =
        new StoreScanner(new Scan(Bytes.toBytes("R1")), CF, Long.MAX_VALUE, KeyValue.COMPARATOR,
            null, scanners);
    List<KeyValue> results = new ArrayList<KeyValue>();
    assertEquals(true, scan.next(results));
    assertEquals(2, results.size());
    assertEquals(kvs[0], results.get(0));
    assertEquals(kvs[1], results.get(1));
  }
  public void testWildCardScannerUnderDeletes() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 2, KeyValue.Type.Put, "dont-care"), // inc
        // orphaned delete column.
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.DeleteColumn, "dont-care"),
        // column b
        KeyValueTestUtil.create("R1", "cf", "b", 2, KeyValue.Type.Put, "dont-care"), // inc
        KeyValueTestUtil.create("R1", "cf", "b", 1, KeyValue.Type.Put, "dont-care"), // inc
        // column c
        KeyValueTestUtil.create("R1", "cf", "c", 10, KeyValue.Type.Delete, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "c", 10, KeyValue.Type.Put, "dont-care"), // no
        KeyValueTestUtil.create("R1", "cf", "c", 9, KeyValue.Type.Put, "dont-care"),  // inc
        // column d
        KeyValueTestUtil.create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"), // inc
        KeyValueTestUtil.create("R1", "cf", "d", 10, KeyValue.Type.DeleteColumn, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "d", 9, KeyValue.Type.Put, "dont-care"),  // no
        KeyValueTestUtil.create("R1", "cf", "d", 8, KeyValue.Type.Put, "dont-care"),  // no

    };
    KeyValueScanner [] scanners = new KeyValueScanner[] {
        new KeyValueScanFixture(KeyValue.COMPARATOR, kvs)
    };
    StoreScanner scan =
        new StoreScanner(new Scan().setMaxVersions(2), CF, Long.MAX_VALUE, KeyValue.COMPARATOR,
            null, scanners);
    List<KeyValue> results = new ArrayList<KeyValue>();
    assertEquals(true, scan.next(results));
    assertEquals(5, results.size());
    assertEquals(kvs[0], results.get(0));
    assertEquals(kvs[2], results.get(1));
    assertEquals(kvs[3], results.get(2));
    assertEquals(kvs[6], results.get(3));
    assertEquals(kvs[7], results.get(4));
  }
  public void testDeleteFamily() throws IOException {
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 100, KeyValue.Type.DeleteFamily, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "e", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "e", 11, KeyValue.Type.DeleteColumn, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "f", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "g", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "g", 11, KeyValue.Type.Delete, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "h", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "i", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", 11, KeyValue.Type.Put, "dont-care"), 
    };
    KeyValueScanner [] scanners = new KeyValueScanner[] {
        new KeyValueScanFixture(KeyValue.COMPARATOR, kvs)
    };
    StoreScanner scan =
        new StoreScanner(new Scan().setMaxVersions(Integer.MAX_VALUE), CF, Long.MAX_VALUE, KeyValue.COMPARATOR,
            null, scanners);
    List<KeyValue> results = new ArrayList<KeyValue>();
    assertEquals(true, scan.next(results));
    assertEquals(0, results.size());
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[kvs.length-1], results.get(0));

    assertEquals(false, scan.next(results));
  }

  public void testDeleteColumn() throws IOException {
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 10, KeyValue.Type.DeleteColumn, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 9, KeyValue.Type.Delete, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 8, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", 5, KeyValue.Type.Put, "dont-care")
    };
    KeyValueScanner [] scanners = new KeyValueScanner[] {
        new KeyValueScanFixture(KeyValue.COMPARATOR, kvs),
    };
    StoreScanner scan =
        new StoreScanner(new Scan(), CF, Long.MAX_VALUE, KeyValue.COMPARATOR,
           null, scanners);
    List<KeyValue> results = new ArrayList<KeyValue>();
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[3], results.get(0));
  }

  public void testSkipColumn() throws IOException {
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "e", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "f", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "g", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "h", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "i", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
    };
    KeyValueScanner [] scanners = new KeyValueScanner[] {
        new KeyValueScanFixture(KeyValue.COMPARATOR, kvs)
    };
    StoreScanner scan =
        new StoreScanner(new Scan(), CF, Long.MAX_VALUE, KeyValue.COMPARATOR,
            getCols("a", "d"), scanners);
    
    List<KeyValue> results = new ArrayList<KeyValue>();
    assertEquals(true, scan.next(results));
    assertEquals(2, results.size());
    assertEquals(kvs[0], results.get(0));
    assertEquals(kvs[3], results.get(1));
    results.clear();

    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[kvs.length-1], results.get(0));
    
    results.clear();
    assertEquals(false, scan.next(results));
  }

  KeyValue [] stdKvs = new KeyValue[] {
      KeyValueTestUtil.create("R:1", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:1", "cf", "b", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:1", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:1", "cf", "e", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:1", "cf", "f", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:1", "cf", "g", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:1", "cf", "h", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:1", "cf", "i", 11, KeyValue.Type.Put, "dont-care"),

      // 9...
      KeyValueTestUtil.create("R:2", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:2", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:2", "cf", "c", 10, KeyValue.Type.Put, "dont-care"),

      // 12...
      KeyValueTestUtil.create("R:3", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:3", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:3", "cf", "c", 10, KeyValue.Type.Put, "dont-care"),

      // 15 ...
      KeyValueTestUtil.create("R:4", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:4", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:4", "cf", "c", 10, KeyValue.Type.Put, "dont-care"),

      // 18 ..
      KeyValueTestUtil.create("R:5", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:5", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),

      // 20...
      KeyValueTestUtil.create("R:6", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:6", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),

      // 22...
      KeyValueTestUtil.create("R:7", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:7", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),

      // 24...
      KeyValueTestUtil.create("R:8", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
      KeyValueTestUtil.create("R:8", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),

      // 26 ..
      KeyValueTestUtil.create("RA:1", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),

      // 27...
      KeyValueTestUtil.create("RA:2", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),

      // 28..
      KeyValueTestUtil.create("RA:3", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
  };
  private StoreScanner getTestScanner(Scan s, NavigableSet<byte[]> cols) {
    KeyValueScanner [] scanners = new KeyValueScanner[] {
        new KeyValueScanFixture(KeyValue.COMPARATOR, stdKvs)
    };
    
    return new StoreScanner(s, CF, Long.MAX_VALUE, KeyValue.COMPARATOR, cols,
        scanners);
  }


  // Test new and old row prefix filters.
  public void testNewRowPrefixFilter() throws IOException {
     Filter f = new WhileMatchFilter(
        new PrefixFilter(Bytes.toBytes("R:")));
    Scan s = new Scan(Bytes.toBytes("R:7"));
    s.setFilter(f);

    rowPrefixFilter(s);
  }
  
  public void testOldRowPrefixFilter() throws IOException {
    RowFilterInterface f = new WhileMatchRowFilter(
        new PrefixRowFilter(Bytes.toBytes("R:")));
    Scan s = new Scan(Bytes.toBytes("R:7"));
    s.setOldFilter(f);

    rowPrefixFilter(s);

  }
  public void rowPrefixFilter(Scan s) throws IOException {

    StoreScanner scan = getTestScanner(s, null);

    List<KeyValue> results = new ArrayList<KeyValue>();
    assertTrue(scan.next(results));
    assertEquals(2, results.size());
    assertEquals(stdKvs[22], results.get(0));
    assertEquals(stdKvs[23], results.get(1));
    results.clear();

    assertTrue(scan.next(results));
    assertEquals(2, results.size());
    assertEquals(stdKvs[24], results.get(0));
    assertEquals(stdKvs[25], results.get(1));
    results.clear();

    assertFalse(scan.next(results));
    assertEquals(0, results.size());
  }

  // Test new and old row-inclusive stop filter.
  public void testNewRowInclusiveStopFilter() throws IOException {
    Filter f = new WhileMatchFilter(new InclusiveStopFilter(Bytes.toBytes("R:3")));
    Scan scan = new Scan();
    scan.setFilter(f);

    rowInclusiveStopFilter(scan);
  }

  public void testOldRowInclusiveTopFilter() throws IOException {
    RowFilterInterface f = new WhileMatchRowFilter(
        new InclusiveStopRowFilter(Bytes.toBytes("R:3")));
    Scan scan = new Scan();
    scan.setOldFilter(f);

    rowInclusiveStopFilter(scan);
  }

  public void rowInclusiveStopFilter(Scan scan) throws IOException {
    StoreScanner s = getTestScanner(scan, getCols("a"));

    // read crap.
    List<KeyValue> results = new ArrayList<KeyValue>();
    assertTrue(s.next(results));
    assertEquals(1, results.size());
    assertEquals(stdKvs[0], results.get(0));
    results.clear();

    assertTrue(s.next(results));
    assertEquals(1, results.size());
    assertEquals(stdKvs[9], results.get(0));
    results.clear();

    assertTrue(s.next(results));
    assertEquals(1, results.size());
    assertEquals(stdKvs[12], results.get(0));
    results.clear();

    // without aggressive peeking, the scanner doesnt know if the next row is good or not
    // under the affects of a filter.
    assertFalse(s.next(results));
    assertEquals(0, results.size());
  }



}
