/**
 * Copyright 2011 The Apache Software Foundation
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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test Minimum Versions feature (HBASE-4071).
 */
public class TestMinVersions extends HBaseTestCase {
  private final byte[] T0 = Bytes.toBytes("0");
  private final byte[] T1 = Bytes.toBytes("1");
  private final byte[] T2 = Bytes.toBytes("2");
  private final byte[] T3 = Bytes.toBytes("3");
  private final byte[] T4 = Bytes.toBytes("4");
  private final byte[] T5 = Bytes.toBytes("5");

  private final byte[] c0 = COLUMNS[0];

  /**
   * Verify behavior of getClosestBefore(...)
   */
  public void testGetClosestBefore() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName(), 1, 1000, 1);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis() - 2000; // 2s in the past

    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);

    p = new Put(T1, ts+1);
    p.add(c0, c0, T4);
    region.put(p);

    p = new Put(T3, ts);
    p.add(c0, c0, T3);
    region.put(p);

    // now make sure that getClosestBefore(...) get can
    // rows that would be expired without minVersion.
    // also make sure it gets the latest version
    Result r = region.getClosestRowBefore(T1, c0);
    checkResult(r, c0, T4);

    r = region.getClosestRowBefore(T2, c0);
    checkResult(r, c0, T4);

    // now flush/compact
    region.flushcache();
    region.compactStores(true);

    r = region.getClosestRowBefore(T1, c0);
    checkResult(r, c0, T4);

    r = region.getClosestRowBefore(T2, c0);
    checkResult(r, c0, T4);
  }

  /**
   * Test mixed memstore and storefile scanning
   * with minimum versions.
   */
  public void testStoreMemStore() throws Exception {
    // keep 3 versions minimum
    HTableDescriptor htd = createTableDescriptor(getName(), 3, 1000, 1);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis() - 2000; // 2s in the past

    Put p = new Put(T1, ts-1);
    p.add(c0, c0, T2);
    region.put(p);

    p = new Put(T1, ts-3);
    p.add(c0, c0, T0);
    region.put(p);

    // now flush/compact
    region.flushcache();
    region.compactStores(true);

    p = new Put(T1, ts);
    p.add(c0, c0, T3);
    region.put(p);

    p = new Put(T1, ts-2);
    p.add(c0, c0, T1);
    region.put(p);

    p = new Put(T1, ts-3);
    p.add(c0, c0, T0);
    region.put(p);

    // newest version in the memstore
    // the 2nd oldest in the store file
    // and the 3rd, 4th oldest also in the memstore

    Get g = new Get(T1);
    g.setMaxVersions();
    Result r = region.get(g, null); // this'll use ScanWildcardColumnTracker
    checkResult(r, c0, T3,T2,T1);

    g = new Get(T1);
    g.setMaxVersions();
    g.addColumn(c0, c0);
    r = region.get(g, null);  // this'll use ExplicitColumnTracker
    checkResult(r, c0, T3,T2,T1);
  }

  /**
   * Make sure the Deletes behave as expected with minimum versions
   */
  public void testDelete() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName(), 3, 1000, 1);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis() - 2000; // 2s in the past

    Put p = new Put(T1, ts-2);
    p.add(c0, c0, T1);
    region.put(p);

    p = new Put(T1, ts-1);
    p.add(c0, c0, T2);
    region.put(p);

    p = new Put(T1, ts);
    p.add(c0, c0, T3);
    region.put(p);

    Delete d = new Delete(T1, ts-1, null);
    region.delete(d, null, true);

    Get g = new Get(T1);
    g.setMaxVersions();
    Result r = region.get(g, null);  // this'll use ScanWildcardColumnTracker
    checkResult(r, c0, T3);

    g = new Get(T1);
    g.setMaxVersions();
    g.addColumn(c0, c0);
    r = region.get(g, null);  // this'll use ExplicitColumnTracker
    checkResult(r, c0, T3);

    // now flush/compact
    region.flushcache();
    region.compactStores(true);

    // try again
    g = new Get(T1);
    g.setMaxVersions();
    r = region.get(g, null);  // this'll use ScanWildcardColumnTracker
    checkResult(r, c0, T3);

    g = new Get(T1);
    g.setMaxVersions();
    g.addColumn(c0, c0);
    r = region.get(g, null);  // this'll use ExplicitColumnTracker
    checkResult(r, c0, T3);
  }

  /**
   * Make sure the memstor behaves correctly with minimum versions
   */
  public void testMemStore() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName(), 2, 1000, 1);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis() - 2000; // 2s in the past

    // 2nd version
    Put p = new Put(T1, ts-2);
    p.add(c0, c0, T2);
    region.put(p);

    // 3rd version
    p = new Put(T1, ts-1);
    p.add(c0, c0, T3);
    region.put(p);

    // 4th version
    p = new Put(T1, ts);
    p.add(c0, c0, T4);
    region.put(p);

    // now flush/compact
    region.flushcache();
    region.compactStores(true);

    // now put the first version (backdated)
    p = new Put(T1, ts-3);
    p.add(c0, c0, T1);
    region.put(p);

    // now the latest change is in the memstore,
    // but it is not the latest version

    Result r = region.get(new Get(T1), null);
    checkResult(r, c0, T4);

    Get g = new Get(T1);
    g.setMaxVersions();
    r = region.get(g, null); // this'll use ScanWildcardColumnTracker
    checkResult(r, c0, T4,T3);

    g = new Get(T1);
    g.setMaxVersions();
    g.addColumn(c0, c0);
    r = region.get(g, null);  // this'll use ExplicitColumnTracker
    checkResult(r, c0, T4,T3);

    p = new Put(T1, ts+1);
    p.add(c0, c0, T5);
    region.put(p);

    // now the latest version is in the memstore

    g = new Get(T1);
    g.setMaxVersions();
    r = region.get(g, null);  // this'll use ScanWildcardColumnTracker
    checkResult(r, c0, T5,T4);

    g = new Get(T1);
    g.setMaxVersions();
    g.addColumn(c0, c0);
    r = region.get(g, null);  // this'll use ExplicitColumnTracker
    checkResult(r, c0, T5,T4);
  }

  /**
   * Verify basic minimum versions functionality
   */
  public void testBaseCase() throws Exception {
    // 1 version minimum, 1000 versions maximum, ttl = 1s
    HTableDescriptor htd = createTableDescriptor(getName(), 2, 1000, 1);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis() - 2000; // 2s in the past

     // 1st version
    Put p = new Put(T1, ts-3);
    p.add(c0, c0, T1);
    region.put(p);

    // 2nd version
    p = new Put(T1, ts-2);
    p.add(c0, c0, T2);
    region.put(p);

    // 3rd version
    p = new Put(T1, ts-1);
    p.add(c0, c0, T3);
    region.put(p);

    // 4th version
    p = new Put(T1, ts);
    p.add(c0, c0, T4);
    region.put(p);

    Result r = region.get(new Get(T1), null);
    checkResult(r, c0, T4);

    Get g = new Get(T1);
    g.setTimeRange(0L, ts+1);
    r = region.get(g, null);
    checkResult(r, c0, T4);

    // oldest version still exists
    g.setTimeRange(0L, ts-2);
    r = region.get(g, null);
    checkResult(r, c0, T1);

    // gets see only available versions
    // even before compactions
    g = new Get(T1);
    g.setMaxVersions();
    r = region.get(g, null); // this'll use ScanWildcardColumnTracker
    checkResult(r, c0, T4,T3);

    g = new Get(T1);
    g.setMaxVersions();
    g.addColumn(c0, c0);
    r = region.get(g, null);  // this'll use ExplicitColumnTracker
    checkResult(r, c0, T4,T3);

    // now flush
    region.flushcache();

    // with HBASE-4241 a flush will eliminate the expired rows
    g = new Get(T1);
    g.setTimeRange(0L, ts-2);
    r = region.get(g, null);
    assertTrue(r.isEmpty());

    // major compaction
    region.compactStores(true);

    // after compaction the 4th version is still available
    g = new Get(T1);
    g.setTimeRange(0L, ts+1);
    r = region.get(g, null);
    checkResult(r, c0, T4);

    // so is the 3rd
    g.setTimeRange(0L, ts);
    r = region.get(g, null);
    checkResult(r, c0, T3);

    // but the 2nd and earlier versions are gone
    g.setTimeRange(0L, ts-1);
    r = region.get(g, null);
    assertTrue(r.isEmpty());
  }

  /**
   * Verify that basic filters still behave correctly with
   * minimum versions enabled.
   */
  public void testFilters() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName(), 2, 1000, 1);
    HRegion region = createNewHRegion(htd, null, null);
    final byte [] c1 = COLUMNS[1];

    long ts = System.currentTimeMillis() - 2000; // 2s in the past

    Put p = new Put(T1, ts-3);
    p.add(c0, c0, T0);
    p.add(c1, c1, T0);
    region.put(p);

    p = new Put(T1, ts-2);
    p.add(c0, c0, T1);
    p.add(c1, c1, T1);
    region.put(p);

    p = new Put(T1, ts-1);
    p.add(c0, c0, T2);
    p.add(c1, c1, T2);
    region.put(p);

    p = new Put(T1, ts);
    p.add(c0, c0, T3);
    p.add(c1, c1, T3);
    region.put(p);

    List<Long> tss = new ArrayList<Long>();
    tss.add(ts-1);
    tss.add(ts-2);

    Get g = new Get(T1);
    g.addColumn(c1,c1);
    g.setFilter(new TimestampsFilter(tss));
    g.setMaxVersions();
    Result r = region.get(g, null);
    checkResult(r, c1, T2,T1);

    g = new Get(T1);
    g.addColumn(c0,c0);
    g.setFilter(new TimestampsFilter(tss));
    g.setMaxVersions();
    r = region.get(g, null);
    checkResult(r, c0, T2,T1);

    // now flush/compact
    region.flushcache();
    region.compactStores(true);

    g = new Get(T1);
    g.addColumn(c1,c1);
    g.setFilter(new TimestampsFilter(tss));
    g.setMaxVersions();
    r = region.get(g, null);
    checkResult(r, c1, T2);

    g = new Get(T1);
    g.addColumn(c0,c0);
    g.setFilter(new TimestampsFilter(tss));
    g.setMaxVersions();
    r = region.get(g, null);
    checkResult(r, c0, T2);
}

  private void checkResult(Result r, byte[] col, byte[] ... vals) {
    assertEquals(r.size(), vals.length);
    List<KeyValue> kvs = r.getColumn(col, col);
    assertEquals(kvs.size(), vals.length);
    for (int i=0;i<vals.length;i++) {
      assertEquals(kvs.get(i).getValue(), vals[i]);
    }
  }
}
