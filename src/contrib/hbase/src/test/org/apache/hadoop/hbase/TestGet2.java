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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.io.Text;


/**
 * {@link TestGet} is a medley of tests of get all done up as a single test.
 * This class 
 */
public class TestGet2 extends HBaseTestCase {
  private MiniDFSCluster miniHdfs;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    this.miniHdfs = new MiniDFSCluster(this.conf, 1, true, null);
  }
  
  /**
   * Tests for HADOOP-2161.
   * @throws Exception
   */
  public void testGetFull() throws Exception {
    HRegion region = null;
    HScannerInterface scanner = null;
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);
      for (int i = 0; i < COLUMNS.length; i++) {
        addContent(region, COLUMNS[i].toString());
      }
      // Find two rows to use doing getFull.
      final Text arbitraryStartRow = new Text("b");
      Text actualStartRow = null;
      final Text arbitraryStopRow = new Text("c");
      Text actualStopRow = null;
      Text [] columns = new Text [] {new Text(COLFAMILY_NAME1)};
      scanner = region.getScanner(columns,
          arbitraryStartRow, HConstants.LATEST_TIMESTAMP,
          new WhileMatchRowFilter(new StopRowFilter(arbitraryStopRow)));
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> value = new TreeMap<Text, byte []>();
      while (scanner.next(key, value)) { 
        if (actualStartRow == null) {
          actualStartRow = new Text(key.getRow());
        } else {
          actualStopRow = key.getRow();
        }
      }
      // Assert I got all out.
      assertColumnsPresent(region, actualStartRow);
      assertColumnsPresent(region, actualStopRow);
      // Force a flush so store files come into play.
      region.flushcache();
      // Assert I got all out.
      assertColumnsPresent(region, actualStartRow);
      assertColumnsPresent(region, actualStopRow);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      if (region != null) {
        try {
          region.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
        region.getLog().closeAndDelete();
      }
    }
  }
  
  /**
   * @throws IOException
   */
  public void testGetAtTimestamp() throws IOException{
    HRegion region = null;
    HRegionIncommon region_incommon = null;
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);
      region_incommon = new HRegionIncommon(region);
      
      long right_now = System.currentTimeMillis();
      long one_second_ago = right_now - 1000;
      
      Text t = new Text("test_row");
      long lockid = region_incommon.startBatchUpdate(t);
      region_incommon.put(lockid, COLUMNS[0], "old text".getBytes());
      region_incommon.commit(lockid, one_second_ago);
 
      lockid = region_incommon.startBatchUpdate(t);
      region_incommon.put(lockid, COLUMNS[0], "new text".getBytes());
      region_incommon.commit(lockid, right_now);

      assertCellValueEquals(region, t, COLUMNS[0], right_now, "new text");
      assertCellValueEquals(region, t, COLUMNS[0], one_second_ago, "old text");
      
      // Force a flush so store files come into play.
      region_incommon.flushcache();

      assertCellValueEquals(region, t, COLUMNS[0], right_now, "new text");
      assertCellValueEquals(region, t, COLUMNS[0], one_second_ago, "old text");

    } finally {
      if (region != null) {
        try {
          region.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
        region.getLog().closeAndDelete();
      }
    }
  }
  
  /** For HADOOP-2443 */
  public void testGetClosestRowBefore() throws IOException{

    HRegion region = null;
    HRegionIncommon region_incommon = null;

    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      HRegionInfo hri = new HRegionInfo(htd, null, null);
      region = createNewHRegion(htd, null, null);
      region_incommon = new HRegionIncommon(region);
     
      // set up some test data
      Text t10 = new Text("010");
      Text t20 = new Text("020");
      Text t30 = new Text("030");
      Text t40 = new Text("040");
      
      long lockid = region_incommon.startBatchUpdate(t10);
      region_incommon.put(lockid, COLUMNS[0], "t10 bytes".getBytes());
      region_incommon.commit(lockid);
      
      lockid = region_incommon.startBatchUpdate(t20);
      region_incommon.put(lockid, COLUMNS[0], "t20 bytes".getBytes());
      region_incommon.commit(lockid);
      
      lockid = region_incommon.startBatchUpdate(t30);
      region_incommon.put(lockid, COLUMNS[0], "t30 bytes".getBytes());
      region_incommon.commit(lockid);
      
      lockid = region_incommon.startBatchUpdate(t40);
      region_incommon.put(lockid, COLUMNS[0], "t40 bytes".getBytes());
      region_incommon.commit(lockid);
      
      // try finding "015"
      Text t15 = new Text("015");
      Map<Text, byte[]> results = 
        region.getClosestRowBefore(t15, HConstants.LATEST_TIMESTAMP);
      assertEquals(new String(results.get(COLUMNS[0])), "t10 bytes");

      // try "020", we should get that row exactly
      results = region.getClosestRowBefore(t20, HConstants.LATEST_TIMESTAMP);
      assertEquals(new String(results.get(COLUMNS[0])), "t20 bytes");

      // try "050", should get stuff from "040"
      Text t50 = new Text("050");
      results = region.getClosestRowBefore(t50, HConstants.LATEST_TIMESTAMP);
      assertEquals(new String(results.get(COLUMNS[0])), "t40 bytes");

      // force a flush
      region.flushcache();

      // try finding "015"      
      results = region.getClosestRowBefore(t15, HConstants.LATEST_TIMESTAMP);
      assertEquals(new String(results.get(COLUMNS[0])), "t10 bytes");

      // try "020", we should get that row exactly
      results = region.getClosestRowBefore(t20, HConstants.LATEST_TIMESTAMP);
      assertEquals(new String(results.get(COLUMNS[0])), "t20 bytes");

      // try "050", should get stuff from "040"
      results = region.getClosestRowBefore(t50, HConstants.LATEST_TIMESTAMP);
      assertEquals(new String(results.get(COLUMNS[0])), "t40 bytes");
    } finally {
      if (region != null) {
        try {
          region.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
        region.getLog().closeAndDelete();
      }
    }
  }
  
  
  private void assertCellValueEquals(final HRegion region, final Text row,
    final Text column, final long timestamp, final String value)
  throws IOException {
    Map<Text, byte[]> result = region.getFull(row, timestamp);
    assertEquals("cell value at a given timestamp", new String(result.get(column)), value);
  }
  
  private void assertColumnsPresent(final HRegion r, final Text row)
  throws IOException {
    Map<Text, byte[]> result = r.getFull(row);
    int columnCount = 0;
    for (Map.Entry<Text, byte[]> e: result.entrySet()) {
      columnCount++;
      String column = e.getKey().toString();
      boolean legitColumn = false;
      for (int i = 0; i < COLUMNS.length; i++) {
        // Assert value is same as row.  This is 'nature' of the data added.
        assertTrue(row.equals(new Text(e.getValue())));
        if (COLUMNS[i].equals(new Text(column))) {
          legitColumn = true;
          break;
        }
      }
      assertTrue("is legit column name", legitColumn);
    }
    assertEquals("count of columns", columnCount, COLUMNS.length);
  }

  @Override
  protected void tearDown() throws Exception {
    if (this.miniHdfs != null) {
      this.miniHdfs.shutdown();
    }
    super.tearDown();
  }
}
