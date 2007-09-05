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
import java.util.TreeMap;

import org.apache.hadoop.io.Text;

/**
 * Tests user specifiable time stamps
 */
public class TestTimestamp extends HBaseTestCase {
  private static final long T0 = 10L;
  private static final long T1 = 100L;
  private static final long T2 = 200L;
  
  private static final String COLUMN_NAME = "contents:";
  private static final String TABLE_NAME = "test";
  private static final String VERSION1 = "version1";
  private static final String LATEST = "latest";
  
  private static final Text COLUMN = new Text(COLUMN_NAME);
  private static final Text[] COLUMNS = {
    COLUMN
  };
  private static final Text TABLE = new Text(TABLE_NAME);
  private static final Text ROW = new Text("row");

  /**
   * Test that delete works according to description in <a
   * href="https://issues.apache.org/jira/browse/HADOOP-1784">hadoop-1784</a>
   * when it comes to timestamps.
   * @throws IOException
   */
  public void testDelete() throws IOException {
    HRegion r = createRegion();
    try {
      HRegionLoader loader = new HRegionLoader(r);
      // Add a couple of values for three different timestamps.
      addContent(loader, COLUMN_NAME, START_KEY_BYTES, new Text("aad"), T0);
      addContent(loader, COLUMN_NAME, START_KEY_BYTES, new Text("aad"), T1);
      addContent(loader, COLUMN_NAME, START_KEY_BYTES, new Text("aad"), T2);
      addContent(loader, COLUMN_NAME, START_KEY_BYTES, new Text("aad"));
      // If I delete w/o specifying a timestamp, this means I'm deleting the
      // latest.
      delete(r, System.currentTimeMillis());
      // Verify that I get back T2 through T0.
    } finally {
      r.close();
      r.getLog().closeAndDelete();
    }
  }
  
  private void delete(final HRegion r, final long ts) throws IOException {
    long lockid = r.startUpdate(ROW);
    r.delete(lockid, COLUMN);
    r.commit(lockid, ts == -1? System.currentTimeMillis(): ts);
  }
  
  /**
   * Test scanning against different timestamps.
   * @throws IOException
   */
  public void testTimestampScanning() throws IOException {
    HRegion r = createRegion();
    try {
      HRegionLoader loader = new HRegionLoader(r);
      // Add a couple of values for three different timestamps.
      addContent(loader, COLUMN_NAME, START_KEY_BYTES, new Text("aad"), T0);
      addContent(loader, COLUMN_NAME, START_KEY_BYTES, new Text("aad"), T1);
      addContent(loader, COLUMN_NAME, START_KEY_BYTES, new Text("aad"));
      // Get count of latest items.
      int count = assertScanContentTimestamp(r, System.currentTimeMillis());
      // Assert I get same count when I scan at each timestamp.
      assertEquals(count, assertScanContentTimestamp(r, T0));
      assertEquals(count, assertScanContentTimestamp(r, T1));
      // Flush everything out to disk and then retry
      r.flushcache(false);
      assertEquals(count, assertScanContentTimestamp(r, T0));
      assertEquals(count, assertScanContentTimestamp(r, T1));
    } finally {
      r.close();
      r.getLog().closeAndDelete();
    }
  }
  
  /*
   * Assert that the scan returns only values < timestamp. 
   * @param r
   * @param ts
   * @return Count of items scanned.
   * @throws IOException
   */
  private int assertScanContentTimestamp(final HRegion r, final long ts)
  throws IOException {
    int count = 0;
    HInternalScannerInterface scanner =
      r.getScanner(COLUMNS, HConstants.EMPTY_START_ROW, ts, null);
    try {
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte []>value = new TreeMap<Text, byte[]>();
      while (scanner.next(key, value)) {
        assertTrue(key.getTimestamp() <= ts);
        Text row = key.getRow();
        assertEquals(row.toString(),
          new String(value.get(COLUMN), HConstants.UTF8_ENCODING));
        count++;
        value.clear();
      }
    } finally {
      scanner.close(); 
    }
    return count;
  }

  /**
   * Basic test of timestamps.
   * TODO: Needs rewrite after hadoop-1784 gets fixed.
   * @throws IOException
   */
  public void testTimestamps() throws IOException {
    MiniHBaseCluster cluster = new MiniHBaseCluster(this.conf, 1);
    try {
      HTable table = createTable();
      
      // store a value specifying an update time
      put(table, VERSION1.getBytes(HConstants.UTF8_ENCODING), T0);
      
      // store a value specifying 'now' as the update time
      put(table, LATEST.getBytes(HConstants.UTF8_ENCODING), -1);
      
      // delete values older than T1
      long lockid = table.startUpdate(ROW);
      table.delete(lockid, COLUMN);
      table.commit(lockid, T1);
      
      // now retrieve...
      assertGets(table);

      // flush everything out to disk
      HRegionServer s = cluster.regionThreads.get(0).getRegionServer();
      for(HRegion r: s.onlineRegions.values() ) {
        r.flushcache(false);
      }
      
      // now retrieve...
      assertGets(table);
      
      // Test scanners
      assertScanCount(table, -1, 1);
      assertScanCount(table, T1, 0);
    } catch (Exception e) {
      cluster.shutdown();
    }
  }
  
  /*
   * Test count of results scanning.
   * @param table
   * @param ts
   * @param expectedCount
   * @throws IOException
   */
  private void assertScanCount(final HTable table, final long ts,
      final int expectedCount)
  throws IOException {
    HScannerInterface scanner = (ts == -1)?
      table.obtainScanner(COLUMNS, HConstants.EMPTY_START_ROW):
      table.obtainScanner(COLUMNS, HConstants.EMPTY_START_ROW, ts);
    try {
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
      int count = 0;
      while(scanner.next(key, results)) {
        count++;
      }
      assertEquals(count, expectedCount);
      assertEquals(results.size(), expectedCount);
      
    } finally {
      scanner.close();
    }
  }
  
  /*
   * Test can do basic gets.
   * Used by testTimestamp above.
   * @param table
   * @throws IOException
   */
  private void assertGets(final HTable table) throws IOException {
    // the most recent version:
    byte[] bytes = table.get(ROW, COLUMN);
    assertTrue(bytes != null && bytes.length != 0);
    assertTrue(LATEST.equals(new String(bytes, HConstants.UTF8_ENCODING)));
    
    // any version <= time T1
    byte[][] values = table.get(ROW, COLUMN, T1, 3);
    assertNull(values);
    
    // the version from T0
    values = table.get(ROW, COLUMN, T0, 3);
    assertTrue(values.length == 1
        && VERSION1.equals(new String(values[0], HConstants.UTF8_ENCODING)));
    
    // three versions older than now
    values = table.get(ROW, COLUMN, 3);
    assertTrue(values.length == 1
        && LATEST.equals(new String(values[0], HConstants.UTF8_ENCODING)));
  }
  
  /*
   *  Put values.
   * @param table
   * @param bytes
   * @param ts
   * @throws IOException
   */
  private void put(final HTable table, final byte [] bytes, final long ts)
  throws IOException {
    long lockid = table.startUpdate(ROW);
    table.put(lockid, COLUMN, bytes);
    if (ts == -1) {
      table.commit(lockid);
    } else {
      table.commit(lockid, ts);
    }
  }
  
  /* 
   * Create a table named TABLE_NAME.
   * @return An instance of an HTable connected to the created table.
   * @throws IOException
   */
  private HTable createTable() throws IOException {
    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    return new HTable(conf, TABLE);
  }
  
  private HRegion createRegion() throws IOException {
    HLog hlog = new HLog(this.localFs, this.testDir, this.conf);
    HTableDescriptor htd = createTableDescriptor(getName());
    htd.addFamily(new HColumnDescriptor(COLUMN_NAME));
    HRegionInfo hri = new HRegionInfo(1, htd, null, null);
    return new HRegion(testDir, hlog, this.localFs, this.conf, hri, null);
  }
}