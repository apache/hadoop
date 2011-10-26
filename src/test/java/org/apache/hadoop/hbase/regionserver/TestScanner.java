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
package org.apache.hadoop.hbase.regionserver;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Test of a long-lived scanner validating as we go.
 */
public class TestScanner extends HBaseTestCase {
  private final Log LOG = LogFactory.getLog(this.getClass());

  private static final byte [] FIRST_ROW = HConstants.EMPTY_START_ROW;
  private static final byte [][] COLS = { HConstants.CATALOG_FAMILY };
  private static final byte [][] EXPLICIT_COLS = {
    HConstants.REGIONINFO_QUALIFIER, HConstants.SERVER_QUALIFIER,
      // TODO ryan
      //HConstants.STARTCODE_QUALIFIER
  };

  static final HTableDescriptor TESTTABLEDESC =
    new HTableDescriptor("testscanner");
  static {
    TESTTABLEDESC.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY,
      10,  // Ten is arbitrary number.  Keep versions to help debuggging.
      Compression.Algorithm.NONE.getName(), false, true, 8 * 1024,
      HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),
      HConstants.REPLICATION_SCOPE_LOCAL));
  }
  /** HRegionInfo for root region */
  public static final HRegionInfo REGION_INFO =
    new HRegionInfo(TESTTABLEDESC.getName(), HConstants.EMPTY_BYTE_ARRAY,
    HConstants.EMPTY_BYTE_ARRAY);

  private static final byte [] ROW_KEY = REGION_INFO.getRegionName();

  private static final long START_CODE = Long.MAX_VALUE;

  private HRegion r;
  private HRegionIncommon region;

  /**
   * Test basic stop row filter works.
   * @throws Exception
   */
  public void testStopRow() throws Exception {
    byte [] startrow = Bytes.toBytes("bbb");
    byte [] stoprow = Bytes.toBytes("ccc");
    try {
      this.r = createNewHRegion(TESTTABLEDESC, null, null);
      addContent(this.r, HConstants.CATALOG_FAMILY);
      List<KeyValue> results = new ArrayList<KeyValue>();
      // Do simple test of getting one row only first.
      Scan scan = new Scan(Bytes.toBytes("abc"), Bytes.toBytes("abd"));
      scan.addFamily(HConstants.CATALOG_FAMILY);

      InternalScanner s = r.getScanner(scan);
      int count = 0;
      while (s.next(results)) {
        count++;
      }
      s.close();
      assertEquals(0, count);
      // Now do something a bit more imvolved.
      scan = new Scan(startrow, stoprow);
      scan.addFamily(HConstants.CATALOG_FAMILY);

      s = r.getScanner(scan);
      count = 0;
      KeyValue kv = null;
      results = new ArrayList<KeyValue>();
      for (boolean first = true; s.next(results);) {
        kv = results.get(0);
        if (first) {
          assertTrue(Bytes.BYTES_COMPARATOR.compare(startrow, kv.getRow()) == 0);
          first = false;
        }
        count++;
      }
      assertTrue(Bytes.BYTES_COMPARATOR.compare(stoprow, kv.getRow()) > 0);
      // We got something back.
      assertTrue(count > 10);
      s.close();
    } finally {
      this.r.close();
      this.r.getLog().closeAndDelete();
    }
  }

  void rowPrefixFilter(Scan scan) throws IOException {
    List<KeyValue> results = new ArrayList<KeyValue>();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    InternalScanner s = r.getScanner(scan);
    boolean hasMore = true;
    while (hasMore) {
      hasMore = s.next(results);
      for (KeyValue kv : results) {
        assertEquals((byte)'a', kv.getRow()[0]);
        assertEquals((byte)'b', kv.getRow()[1]);
      }
      results.clear();
    }
    s.close();
  }

  void rowInclusiveStopFilter(Scan scan, byte[] stopRow) throws IOException {
    List<KeyValue> results = new ArrayList<KeyValue>();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    InternalScanner s = r.getScanner(scan);
    boolean hasMore = true;
    while (hasMore) {
      hasMore = s.next(results);
      for (KeyValue kv : results) {
        assertTrue(Bytes.compareTo(kv.getRow(), stopRow) <= 0);
      }
      results.clear();
    }
    s.close();
  }

  public void testFilters() throws IOException {
    try {
      this.r = createNewHRegion(TESTTABLEDESC, null, null);
      addContent(this.r, HConstants.CATALOG_FAMILY);
      byte [] prefix = Bytes.toBytes("ab");
      Filter newFilter = new PrefixFilter(prefix);
      Scan scan = new Scan();
      scan.setFilter(newFilter);
      rowPrefixFilter(scan);

      byte[] stopRow = Bytes.toBytes("bbc");
      newFilter = new WhileMatchFilter(new InclusiveStopFilter(stopRow));
      scan = new Scan();
      scan.setFilter(newFilter);
      rowInclusiveStopFilter(scan, stopRow);

    } finally {
      this.r.close();
      this.r.getLog().closeAndDelete();
    }
  }

  /**
   * Test that closing a scanner while a client is using it doesn't throw
   * NPEs but instead a UnknownScannerException. HBASE-2503
   * @throws Exception
   */
  public void testRaceBetweenClientAndTimeout() throws Exception {
    try {
      this.r = createNewHRegion(TESTTABLEDESC, null, null);
      addContent(this.r, HConstants.CATALOG_FAMILY);
      Scan scan = new Scan();
      InternalScanner s = r.getScanner(scan);
      List<KeyValue> results = new ArrayList<KeyValue>();
      try {
        s.next(results);
        s.close();
        s.next(results);
        fail("We don't want anything more, we should be failing");
      } catch (UnknownScannerException ex) {
        // ok!
        return;
      }
    } finally {
      this.r.close();
      this.r.getLog().closeAndDelete();
    }
  }

  /** The test!
   * @throws IOException
   */
  public void testScanner() throws IOException {
    try {
      r = createNewHRegion(TESTTABLEDESC, null, null);
      region = new HRegionIncommon(r);

      // Write information to the meta table

      Put put = new Put(ROW_KEY, System.currentTimeMillis(), null);

      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(byteStream);
      REGION_INFO.write(s);
      put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
          byteStream.toByteArray());
      region.put(put);

      // What we just committed is in the memstore. Verify that we can get
      // it back both with scanning and get

      scan(false, null);
      getRegionInfo();

      // Close and re-open

      r.close();
      r = openClosedRegion(r);
      region = new HRegionIncommon(r);

      // Verify we can get the data back now that it is on disk.

      scan(false, null);
      getRegionInfo();

      // Store some new information

      String address = "foo.bar.com:1234";

      put = new Put(ROW_KEY, System.currentTimeMillis(), null);
      put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
          Bytes.toBytes(address));

//      put.add(HConstants.COL_STARTCODE, Bytes.toBytes(START_CODE));

      region.put(put);

      // Validate that we can still get the HRegionInfo, even though it is in
      // an older row on disk and there is a newer row in the memstore

      scan(true, address.toString());
      getRegionInfo();

      // flush cache

      region.flushcache();

      // Validate again

      scan(true, address.toString());
      getRegionInfo();

      // Close and reopen

      r.close();
      r = openClosedRegion(r);
      region = new HRegionIncommon(r);

      // Validate again

      scan(true, address.toString());
      getRegionInfo();

      // Now update the information again

      address = "bar.foo.com:4321";

      put = new Put(ROW_KEY, System.currentTimeMillis(), null);

      put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
          Bytes.toBytes(address));
      region.put(put);

      // Validate again

      scan(true, address.toString());
      getRegionInfo();

      // flush cache

      region.flushcache();

      // Validate again

      scan(true, address.toString());
      getRegionInfo();

      // Close and reopen

      r.close();
      r = openClosedRegion(r);
      region = new HRegionIncommon(r);

      // Validate again

      scan(true, address.toString());
      getRegionInfo();

    } finally {
      // clean up
      r.close();
      r.getLog().closeAndDelete();
    }
  }

  /** Compare the HRegionInfo we read from HBase to what we stored */
  private void validateRegionInfo(byte [] regionBytes) throws IOException {
    HRegionInfo info =
      (HRegionInfo) Writables.getWritable(regionBytes, new HRegionInfo());

    assertEquals(REGION_INFO.getRegionId(), info.getRegionId());
    assertEquals(0, info.getStartKey().length);
    assertEquals(0, info.getEndKey().length);
    assertEquals(0, Bytes.compareTo(info.getRegionName(), REGION_INFO.getRegionName()));
    //assertEquals(0, info.getTableDesc().compareTo(REGION_INFO.getTableDesc()));
  }

  /** Use a scanner to get the region info and then validate the results */
  private void scan(boolean validateStartcode, String serverName)
  throws IOException {
    InternalScanner scanner = null;
    Scan scan = null;
    List<KeyValue> results = new ArrayList<KeyValue>();
    byte [][][] scanColumns = {
        COLS,
        EXPLICIT_COLS
    };

    for(int i = 0; i < scanColumns.length; i++) {
      try {
        scan = new Scan(FIRST_ROW);
        for (int ii = 0; ii < EXPLICIT_COLS.length; ii++) {
          scan.addColumn(COLS[0],  EXPLICIT_COLS[ii]);
        }
        scanner = r.getScanner(scan);
        while (scanner.next(results)) {
          assertTrue(hasColumn(results, HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER));
          byte [] val = getColumn(results, HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER).getValue();
          validateRegionInfo(val);
          if(validateStartcode) {
//            assertTrue(hasColumn(results, HConstants.CATALOG_FAMILY,
//                HConstants.STARTCODE_QUALIFIER));
//            val = getColumn(results, HConstants.CATALOG_FAMILY,
//                HConstants.STARTCODE_QUALIFIER).getValue();
            assertNotNull(val);
            assertFalse(val.length == 0);
            long startCode = Bytes.toLong(val);
            assertEquals(START_CODE, startCode);
          }

          if(serverName != null) {
            assertTrue(hasColumn(results, HConstants.CATALOG_FAMILY,
                HConstants.SERVER_QUALIFIER));
            val = getColumn(results, HConstants.CATALOG_FAMILY,
                HConstants.SERVER_QUALIFIER).getValue();
            assertNotNull(val);
            assertFalse(val.length == 0);
            String server = Bytes.toString(val);
            assertEquals(0, server.compareTo(serverName));
          }
        }
      } finally {
        InternalScanner s = scanner;
        scanner = null;
        if(s != null) {
          s.close();
        }
      }
    }
  }

  private boolean hasColumn(final List<KeyValue> kvs, final byte [] family,
      final byte [] qualifier) {
    for (KeyValue kv: kvs) {
      if (kv.matchingFamily(family) && kv.matchingQualifier(qualifier)) {
        return true;
      }
    }
    return false;
  }

  private KeyValue getColumn(final List<KeyValue> kvs, final byte [] family,
      final byte [] qualifier) {
    for (KeyValue kv: kvs) {
      if (kv.matchingFamily(family) && kv.matchingQualifier(qualifier)) {
        return kv;
      }
    }
    return null;
  }


  /** Use get to retrieve the HRegionInfo and validate it */
  private void getRegionInfo() throws IOException {
    Get get = new Get(ROW_KEY);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    Result result = region.get(get, null);
    byte [] bytes = result.value();
    validateRegionInfo(bytes);
  }

  /**
   * Tests to do a sync flush during the middle of a scan. This is testing the StoreScanner
   * update readers code essentially.  This is not highly concurrent, since its all 1 thread.
   * HBase-910.
   * @throws Exception
   */
  public void testScanAndSyncFlush() throws Exception {
    this.r = createNewHRegion(TESTTABLEDESC, null, null);
    HRegionIncommon hri = new HRegionIncommon(r);
    try {
        LOG.info("Added: " + addContent(hri, Bytes.toString(HConstants.CATALOG_FAMILY),
            Bytes.toString(HConstants.REGIONINFO_QUALIFIER)));
      int count = count(hri, -1, false);
      assertEquals(count, count(hri, 100, false)); // do a sync flush.
    } catch (Exception e) {
      LOG.error("Failed", e);
      throw e;
    } finally {
      this.r.close();
      this.r.getLog().closeAndDelete();
    }
  }

  /**
   * Tests to do a concurrent flush (using a 2nd thread) while scanning.  This tests both
   * the StoreScanner update readers and the transition from memstore -> snapshot -> store file.
   *
   * @throws Exception
   */
  public void testScanAndRealConcurrentFlush() throws Exception {
    this.r = createNewHRegion(TESTTABLEDESC, null, null);
    HRegionIncommon hri = new HRegionIncommon(r);
    try {
        LOG.info("Added: " + addContent(hri, Bytes.toString(HConstants.CATALOG_FAMILY),
            Bytes.toString(HConstants.REGIONINFO_QUALIFIER)));
      int count = count(hri, -1, false);
      assertEquals(count, count(hri, 100, true)); // do a true concurrent background thread flush
    } catch (Exception e) {
      LOG.error("Failed", e);
      throw e;
    } finally {
      this.r.close();
      this.r.getLog().closeAndDelete();
    }
  }


  /*
   * @param hri Region
   * @param flushIndex At what row we start the flush.
   * @param concurrent if the flush should be concurrent or sync.
   * @return Count of rows found.
   * @throws IOException
   */
  private int count(final HRegionIncommon hri, final int flushIndex,
                    boolean concurrent)
  throws IOException {
    LOG.info("Taking out counting scan");
    ScannerIncommon s = hri.getScanner(HConstants.CATALOG_FAMILY, EXPLICIT_COLS,
        HConstants.EMPTY_START_ROW, HConstants.LATEST_TIMESTAMP);
    List<KeyValue> values = new ArrayList<KeyValue>();
    int count = 0;
    boolean justFlushed = false;
    while (s.next(values)) {
      if (justFlushed) {
        LOG.info("after next() just after next flush");
        justFlushed=false;
      }
      count++;
      if (flushIndex == count) {
        LOG.info("Starting flush at flush index " + flushIndex);
        Thread t = new Thread() {
          public void run() {
            try {
              hri.flushcache();
              LOG.info("Finishing flush");
            } catch (IOException e) {
              LOG.info("Failed flush cache");
            }
          }
        };
        if (concurrent) {
          t.start(); // concurrently flush.
        } else {
          t.run(); // sync flush
        }
        LOG.info("Continuing on after kicking off background flush");
        justFlushed = true;
      }
    }
    s.close();
    LOG.info("Found " + count + " items");
    return count;
  }
}
