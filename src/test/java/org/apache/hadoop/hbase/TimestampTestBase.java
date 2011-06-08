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

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests user specifiable time stamps putting, getting and scanning.  Also
 * tests same in presence of deletes.  Test cores are written so can be
 * run against an HRegion and against an HTable: i.e. both local and remote.
 */
public class TimestampTestBase extends HBaseTestCase {
  private static final long T0 = 10L;
  private static final long T1 = 100L;
  private static final long T2 = 200L;

  private static final byte [] FAMILY_NAME = Bytes.toBytes("colfamily1");
  private static final byte [] QUALIFIER_NAME = Bytes.toBytes("contents");

  private static final byte [] ROW = Bytes.toBytes("row");

    /*
   * Run test that delete works according to description in <a
   * href="https://issues.apache.org/jira/browse/HADOOP-1784">hadoop-1784</a>.
   * @param incommon
   * @param flusher
   * @throws IOException
   */
  public static void doTestDelete(final Incommon incommon, FlushCache flusher)
  throws IOException {
    // Add values at various timestamps (Values are timestampes as bytes).
    put(incommon, T0);
    put(incommon, T1);
    put(incommon, T2);
    put(incommon);
    // Verify that returned versions match passed timestamps.
    assertVersions(incommon, new long [] {HConstants.LATEST_TIMESTAMP, T2, T1});

    // If I delete w/o specifying a timestamp, this means I'm deleting the
    // latest.
    delete(incommon);
    // Verify that I get back T2 through T1 -- that the latest version has
    // been deleted.
    assertVersions(incommon, new long [] {T2, T1, T0});

    // Flush everything out to disk and then retry
    flusher.flushcache();
    assertVersions(incommon, new long [] {T2, T1, T0});

    // Now add, back a latest so I can test remove other than the latest.
    put(incommon);
    assertVersions(incommon, new long [] {HConstants.LATEST_TIMESTAMP, T2, T1});
    delete(incommon, T2);
    assertVersions(incommon, new long [] {HConstants.LATEST_TIMESTAMP, T1, T0});
    // Flush everything out to disk and then retry
    flusher.flushcache();
    assertVersions(incommon, new long [] {HConstants.LATEST_TIMESTAMP, T1, T0});

    // Now try deleting all from T2 back inclusive (We first need to add T2
    // back into the mix and to make things a little interesting, delete and
    // then readd T1.
    put(incommon, T2);
    delete(incommon, T1);
    put(incommon, T1);

    Delete delete = new Delete(ROW);
    delete.deleteColumns(FAMILY_NAME, QUALIFIER_NAME, T2);
    incommon.delete(delete, null, true);

    // Should only be current value in set.  Assert this is so
    assertOnlyLatest(incommon, HConstants.LATEST_TIMESTAMP);

    // Flush everything out to disk and then redo above tests
    flusher.flushcache();
    assertOnlyLatest(incommon, HConstants.LATEST_TIMESTAMP);
  }

  private static void assertOnlyLatest(final Incommon incommon,
    final long currentTime)
  throws IOException {
    Get get = null;
    get = new Get(ROW);
    get.addColumn(FAMILY_NAME, QUALIFIER_NAME);
    get.setMaxVersions(3);
    Result result = incommon.get(get);
    assertEquals(1, result.size());
    long time = Bytes.toLong(result.sorted()[0].getValue());
    assertEquals(time, currentTime);
  }

  /*
   * Assert that returned versions match passed in timestamps and that results
   * are returned in the right order.  Assert that values when converted to
   * longs match the corresponding passed timestamp.
   * @param r
   * @param tss
   * @throws IOException
   */
  public static void assertVersions(final Incommon incommon, final long [] tss)
  throws IOException {
    // Assert that 'latest' is what we expect.
    Get get = null;
    get = new Get(ROW);
    get.addColumn(FAMILY_NAME, QUALIFIER_NAME);
    Result r = incommon.get(get);
    byte [] bytes = r.getValue(FAMILY_NAME, QUALIFIER_NAME);
    long t = Bytes.toLong(bytes);
    assertEquals(tss[0], t);

    // Now assert that if we ask for multiple versions, that they come out in
    // order.
    get = new Get(ROW);
    get.addColumn(FAMILY_NAME, QUALIFIER_NAME);
    get.setMaxVersions(tss.length);
    Result result = incommon.get(get);
    KeyValue [] kvs = result.sorted();
    assertEquals(kvs.length, tss.length);
    for(int i=0;i<kvs.length;i++) {
      t = Bytes.toLong(kvs[i].getValue());
      assertEquals(tss[i], t);
    }

    // Determine highest stamp to set as next max stamp
    long maxStamp = kvs[0].getTimestamp();

    // Specify a timestamp get multiple versions.
    get = new Get(ROW);
    get.addColumn(FAMILY_NAME, QUALIFIER_NAME);
    get.setTimeRange(0, maxStamp);
    get.setMaxVersions(kvs.length - 1);
    result = incommon.get(get);
    kvs = result.sorted();
    assertEquals(kvs.length, tss.length - 1);
    for(int i=1;i<kvs.length;i++) {
      t = Bytes.toLong(kvs[i-1].getValue());
      assertEquals(tss[i], t);
    }

    // Test scanner returns expected version
    assertScanContentTimestamp(incommon, tss[0]);
  }

  /*
   * Run test scanning different timestamps.
   * @param incommon
   * @param flusher
   * @throws IOException
   */
  public static void doTestTimestampScanning(final Incommon incommon,
    final FlushCache flusher)
  throws IOException {
    // Add a couple of values for three different timestamps.
    put(incommon, T0);
    put(incommon, T1);
    put(incommon, HConstants.LATEST_TIMESTAMP);
    // Get count of latest items.
    int count = assertScanContentTimestamp(incommon,
      HConstants.LATEST_TIMESTAMP);
    // Assert I get same count when I scan at each timestamp.
    assertEquals(count, assertScanContentTimestamp(incommon, T0));
    assertEquals(count, assertScanContentTimestamp(incommon, T1));
    // Flush everything out to disk and then retry
    flusher.flushcache();
    assertEquals(count, assertScanContentTimestamp(incommon, T0));
    assertEquals(count, assertScanContentTimestamp(incommon, T1));
  }

  /*
   * Assert that the scan returns only values < timestamp.
   * @param r
   * @param ts
   * @return Count of items scanned.
   * @throws IOException
   */
  public static int assertScanContentTimestamp(final Incommon in, final long ts)
  throws IOException {
    ScannerIncommon scanner =
      in.getScanner(COLUMNS[0], null, HConstants.EMPTY_START_ROW, ts);
    int count = 0;
    try {
      // TODO FIX
//      HStoreKey key = new HStoreKey();
//      TreeMap<byte [], Cell>value =
//        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
//      while (scanner.next(key, value)) {
//        assertTrue(key.getTimestamp() <= ts);
//        // Content matches the key or HConstants.LATEST_TIMESTAMP.
//        // (Key does not match content if we 'put' with LATEST_TIMESTAMP).
//        long l = Bytes.toLong(value.get(COLUMN).getValue());
//        assertTrue(key.getTimestamp() == l ||
//          HConstants.LATEST_TIMESTAMP == l);
//        count++;
//        value.clear();
//      }
    } finally {
      scanner.close();
    }
    return count;
  }

  public static void put(final Incommon loader, final long ts)
  throws IOException {
    put(loader, Bytes.toBytes(ts), ts);
  }

  public static void put(final Incommon loader)
  throws IOException {
    long ts = HConstants.LATEST_TIMESTAMP;
    put(loader, Bytes.toBytes(ts), ts);
  }

  /*
   * Put values.
   * @param loader
   * @param bytes
   * @param ts
   * @throws IOException
   */
  public static void put(final Incommon loader, final byte [] bytes,
    final long ts)
  throws IOException {
    Put put = new Put(ROW, ts, null);
    put.add(FAMILY_NAME, QUALIFIER_NAME, bytes);
    loader.put(put);
  }

  public static void delete(final Incommon loader) throws IOException {
    delete(loader, null);
  }

  public static void delete(final Incommon loader, final byte [] column)
  throws IOException {
    delete(loader, column, HConstants.LATEST_TIMESTAMP);
  }

  public static void delete(final Incommon loader, final long ts)
  throws IOException {
    delete(loader, null, ts);
  }

  public static void delete(final Incommon loader, final byte [] column,
      final long ts)
  throws IOException {
    Delete delete = ts == HConstants.LATEST_TIMESTAMP?
      new Delete(ROW): new Delete(ROW, ts, null);
    delete.deleteColumn(FAMILY_NAME, QUALIFIER_NAME, ts);
    loader.delete(delete, null, true);
  }

  public static Result get(final Incommon loader) throws IOException {
    return loader.get(new Get(ROW));
  }
}