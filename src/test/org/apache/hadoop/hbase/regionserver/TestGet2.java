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

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * {@link TestGet} is a medley of tests of get all done up as a single test.
 * This class 
 */
public class TestGet2 extends HBaseTestCase implements HConstants {
  private MiniDFSCluster miniHdfs;
  
  private static final String T00 = "000";
  private static final String T10 = "010";
  private static final String T11 = "011";
  private static final String T12 = "012";
  private static final String T20 = "020";
  private static final String T30 = "030";
  private static final String T31 = "031";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    this.miniHdfs = new MiniDFSCluster(this.conf, 1, true, null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR,
      this.miniHdfs.getFileSystem().getHomeDirectory().toString());
  }


  public void testGetFullMultiMapfile() throws IOException {
    HRegion region = null;
    BatchUpdate batchUpdate = null;
    Map<byte [], Cell> results = null;
    
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);

      // Test ordering issue
      //
      byte [] row = Bytes.toBytes("row1");
     
      // write some data
      batchUpdate = new BatchUpdate(row);
      batchUpdate.put(COLUMNS[0], "olderValue".getBytes());
      region.batchUpdate(batchUpdate, null);

      // flush
      region.flushcache();

      // assert that getFull gives us the older value
      results = region.getFull(row, (Set<byte []>)null, LATEST_TIMESTAMP, 1, null);
      assertEquals("olderValue", new String(results.get(COLUMNS[0]).getValue()));
      
      // write a new value for the cell
      batchUpdate = new BatchUpdate(row);
      batchUpdate.put(COLUMNS[0], "newerValue".getBytes());
      region.batchUpdate(batchUpdate, null);

      // flush
      region.flushcache();
      
      // assert that getFull gives us the later value
      results = region.getFull(row, (Set<byte []>)null, LATEST_TIMESTAMP, 1, null);
      assertEquals("newerValue", new String(results.get(COLUMNS[0]).getValue()));
     
      //
      // Test the delete masking issue
      //
      byte [] row2 = Bytes.toBytes("row2");
      byte [] cell1 = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "a");
      byte [] cell2 = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "b");
      byte [] cell3 = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "c");
      
      // write some data at two columns
      batchUpdate = new BatchUpdate(row2);
      batchUpdate.put(cell1, "column0 value".getBytes());
      batchUpdate.put(cell2, "column1 value".getBytes());
      region.batchUpdate(batchUpdate, null);
      
      // flush
      region.flushcache();
      
      // assert i get both columns
      results = region.getFull(row2, (Set<byte []>)null, LATEST_TIMESTAMP, 1, null);
      assertEquals("Should have two columns in the results map", 2, results.size());
      assertEquals("column0 value", new String(results.get(cell1).getValue()));
      assertEquals("column1 value", new String(results.get(cell2).getValue()));
      
      // write a delete for the first column
      batchUpdate = new BatchUpdate(row2);
      batchUpdate.delete(cell1);
      batchUpdate.put(cell2, "column1 new value".getBytes());      
      region.batchUpdate(batchUpdate, null);
            
      // flush
      region.flushcache(); 
      
      // assert i get the second column only
      results = region.getFull(row2, (Set<byte []>)null, LATEST_TIMESTAMP, 1, null);
      System.out.println(Bytes.toString(results.keySet().iterator().next()));
      assertEquals("Should have one column in the results map", 1, results.size());
      assertNull("column0 value", results.get(cell1));
      assertEquals("column1 new value", new String(results.get(cell2).getValue()));
      
      //
      // Include a delete and value from the memcache in the mix
      //
      batchUpdate = new BatchUpdate(row2);
      batchUpdate.delete(cell2);
      batchUpdate.put(cell3, "column3 value!".getBytes());
      region.batchUpdate(batchUpdate, null);
      
      // assert i get the third column only
      results = region.getFull(row2, (Set<byte []>)null, LATEST_TIMESTAMP, 1, null);
      assertEquals("Should have one column in the results map", 1, results.size());
      assertNull("column0 value", results.get(cell1));
      assertNull("column1 value", results.get(cell2));
      assertEquals("column3 value!", new String(results.get(cell3).getValue()));
      
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

  /** For HBASE-694 
   * @throws IOException
   */
  public void testGetClosestRowBefore2() throws IOException {

    HRegion region = null;
    BatchUpdate batchUpdate = null;
    
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);
     
      // set up some test data
      String t10 = "010";
      String t20 = "020";
      String t30 = "030";
      String t40 = "040";
      
      batchUpdate = new BatchUpdate(t10);
      batchUpdate.put(COLUMNS[0], "t10 bytes".getBytes());
      region.batchUpdate(batchUpdate, null);
      
      batchUpdate = new BatchUpdate(t30);
      batchUpdate.put(COLUMNS[0], "t30 bytes".getBytes());
      region.batchUpdate(batchUpdate, null);
      
      batchUpdate = new BatchUpdate(t40);
      batchUpdate.put(COLUMNS[0], "t40 bytes".getBytes());
      region.batchUpdate(batchUpdate, null);

      // try finding "035"
      String t35 = "035";
      Map<byte [], Cell> results = 
        region.getClosestRowBefore(Bytes.toBytes(t35), COLUMNS[0]);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t30 bytes");

      region.flushcache();

      // try finding "035"
      results = region.getClosestRowBefore(Bytes.toBytes(t35), COLUMNS[0]);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t30 bytes");

      batchUpdate = new BatchUpdate(t20);
      batchUpdate.put(COLUMNS[0], "t20 bytes".getBytes());
      region.batchUpdate(batchUpdate, null);

      // try finding "035"
      results = region.getClosestRowBefore(Bytes.toBytes(t35), COLUMNS[0]);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t30 bytes");

      region.flushcache();

      // try finding "035"
      results = region.getClosestRowBefore(Bytes.toBytes(t35), COLUMNS[0]);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t30 bytes");
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

  /**
   * Test for HBASE-808 and HBASE-809.
   * @throws Exception
   */
  public void testMaxVersionsAndDeleting() throws Exception {
    HRegion region = null;
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);
      
      byte [] column = COLUMNS[0];
      for (int i = 0; i < 100; i++) {
        addToRow(region, T00, column, i, T00.getBytes());
      }
      checkVersions(region, T00, column);
      // Flush and retry.
      region.flushcache();
      checkVersions(region, T00, column);
      
      // Now delete all then retry
      region.deleteAll(Bytes.toBytes(T00), System.currentTimeMillis(), null);
      Cell [] cells = region.get(Bytes.toBytes(T00), column, -1,
        HColumnDescriptor.DEFAULT_VERSIONS);
      assertTrue(cells == null);
      region.flushcache();
      cells = region.get(Bytes.toBytes(T00), column, -1,
          HColumnDescriptor.DEFAULT_VERSIONS);
      assertTrue(cells == null);
      
      // Now add back the rows
      for (int i = 0; i < 100; i++) {
        addToRow(region, T00, column, i, T00.getBytes());
      }
      // Run same verifications.
      checkVersions(region, T00, column);
      // Flush and retry.
      region.flushcache();
      checkVersions(region, T00, column);
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
  
  private void addToRow(final HRegion r, final String row, final byte [] column,
      final long ts, final byte [] bytes)
  throws IOException {
    BatchUpdate batchUpdate = new BatchUpdate(row, ts);
    batchUpdate.put(column, bytes);
    r.batchUpdate(batchUpdate, null);
  }

  private void checkVersions(final HRegion region, final String row,
      final byte [] column)
  throws IOException {
    byte [] r = Bytes.toBytes(row);
    Cell [] cells = region.get(r, column, -1, 100);
    assertTrue(cells.length == HColumnDescriptor.DEFAULT_VERSIONS);
    cells = region.get(r, column, -1, 1);
    assertTrue(cells.length == 1);
    cells = region.get(r, column, -1, HConstants.ALL_VERSIONS);
    assertTrue(cells.length == HColumnDescriptor.DEFAULT_VERSIONS);
  }
  
  /**
   * Test file of multiple deletes and with deletes as final key.
   * @throws IOException 
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-751">HBASE-751</a>
   */
  public void testGetClosestRowBefore3() throws IOException {
    HRegion region = null;
    BatchUpdate batchUpdate = null;
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);
      
      batchUpdate = new BatchUpdate(T00);
      batchUpdate.put(COLUMNS[0], T00.getBytes());
      region.batchUpdate(batchUpdate, null);
      
      batchUpdate = new BatchUpdate(T10);
      batchUpdate.put(COLUMNS[0], T10.getBytes());
      region.batchUpdate(batchUpdate, null);
      
      batchUpdate = new BatchUpdate(T20);
      batchUpdate.put(COLUMNS[0], T20.getBytes());
      region.batchUpdate(batchUpdate, null);
      
      Map<byte [], Cell> results =
        region.getClosestRowBefore(Bytes.toBytes(T20), COLUMNS[0]);
      assertEquals(T20, new String(results.get(COLUMNS[0]).getValue()));
      
      batchUpdate = new BatchUpdate(T20);
      batchUpdate.delete(COLUMNS[0]);
      region.batchUpdate(batchUpdate, null);
      
      results = region.getClosestRowBefore(Bytes.toBytes(T20), COLUMNS[0]);
      assertEquals(T10, new String(results.get(COLUMNS[0]).getValue()));
      
      batchUpdate = new BatchUpdate(T30);
      batchUpdate.put(COLUMNS[0], T30.getBytes());
      region.batchUpdate(batchUpdate, null);
      
      results = region.getClosestRowBefore(Bytes.toBytes(T30), COLUMNS[0]);
      assertEquals(T30, new String(results.get(COLUMNS[0]).getValue()));
      
      batchUpdate = new BatchUpdate(T30);
      batchUpdate.delete(COLUMNS[0]);
      region.batchUpdate(batchUpdate, null);

      results = region.getClosestRowBefore(Bytes.toBytes(T30), COLUMNS[0]);
      assertEquals(T10, new String(results.get(COLUMNS[0]).getValue()));
      results = region.getClosestRowBefore(Bytes.toBytes(T31), COLUMNS[0]);
      assertEquals(T10, new String(results.get(COLUMNS[0]).getValue()));

      region.flushcache();

      // try finding "010" after flush
      results = region.getClosestRowBefore(Bytes.toBytes(T30), COLUMNS[0]);
      assertEquals(T10, new String(results.get(COLUMNS[0]).getValue()));
      results = region.getClosestRowBefore(Bytes.toBytes(T31), COLUMNS[0]);
      assertEquals(T10, new String(results.get(COLUMNS[0]).getValue()));
      
      // Put into a different column family.  Should make it so I still get t10
      batchUpdate = new BatchUpdate(T20);
      batchUpdate.put(COLUMNS[1], T20.getBytes());
      region.batchUpdate(batchUpdate, null);
      
      results = region.getClosestRowBefore(Bytes.toBytes(T30), COLUMNS[0]);
      assertEquals(T10, new String(results.get(COLUMNS[0]).getValue()));
      results = region.getClosestRowBefore(Bytes.toBytes(T31), COLUMNS[0]);
      assertEquals(T10, new String(results.get(COLUMNS[0]).getValue()));
      region.flushcache();
      results = region.getClosestRowBefore(Bytes.toBytes(T30), COLUMNS[0]);
      assertEquals(T10, new String(results.get(COLUMNS[0]).getValue()));
      results = region.getClosestRowBefore(Bytes.toBytes(T31), COLUMNS[0]);
      assertEquals(T10, new String(results.get(COLUMNS[0]).getValue()));
      
      // Now try combo of memcache and mapfiles.  Delete the t20 COLUMS[1]
      // in memory; make sure we get back t10 again.
      batchUpdate = new BatchUpdate(T20);
      batchUpdate.delete(COLUMNS[1]);
      region.batchUpdate(batchUpdate, null);
      results = region.getClosestRowBefore(Bytes.toBytes(T30), COLUMNS[0]);
      assertEquals(T10, new String(results.get(COLUMNS[0]).getValue()));
      
      // Ask for a value off the end of the file.  Should return t10.
      results = region.getClosestRowBefore(Bytes.toBytes(T31), COLUMNS[0]);
      assertEquals(T10, new String(results.get(COLUMNS[0]).getValue()));
      region.flushcache();
      results = region.getClosestRowBefore(Bytes.toBytes(T31), COLUMNS[0]);
      assertEquals(T10, new String(results.get(COLUMNS[0]).getValue()));
      
      // Ok.  Let the candidate come out of mapfiles but have delete of
      // the candidate be in memory.
      batchUpdate = new BatchUpdate(T11);
      batchUpdate.put(COLUMNS[0], T11.getBytes());
      region.batchUpdate(batchUpdate, null);
      batchUpdate = new BatchUpdate(T10);
      batchUpdate.delete(COLUMNS[0]);
      region.batchUpdate(batchUpdate, null);
      results = region.getClosestRowBefore(Bytes.toBytes(T12), COLUMNS[0]);
      assertEquals(T11, new String(results.get(COLUMNS[0]).getValue()));
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

  /**
   * Tests for HADOOP-2161.
   * @throws Exception
   */
  public void testGetFull() throws Exception {
    HRegion region = null;
    InternalScanner scanner = null;
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);
      for (int i = 0; i < COLUMNS.length; i++) {
        addContent(region, COLUMNS[i]);
      }
      // Find two rows to use doing getFull.
      final byte [] arbitraryStartRow = Bytes.toBytes("b");
      byte [] actualStartRow = null;
      final byte [] arbitraryStopRow = Bytes.toBytes("c");
      byte [] actualStopRow = null;
      byte [][] columns = {COLFAMILY_NAME1};
      scanner = region.getScanner(columns,
          arbitraryStartRow, HConstants.LATEST_TIMESTAMP,
          new WhileMatchRowFilter(new StopRowFilter(arbitraryStopRow)));
      HStoreKey key = new HStoreKey();
      TreeMap<byte [], Cell> value =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      while (scanner.next(key, value)) { 
        if (actualStartRow == null) {
          actualStartRow = key.getRow();
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
      
      String t = "test_row";
      BatchUpdate batchUpdate = new BatchUpdate(t, one_second_ago);
      batchUpdate.put(COLUMNS[0], "old text".getBytes());
      region_incommon.commit(batchUpdate);
 
      batchUpdate = new BatchUpdate(t, right_now);
      batchUpdate.put(COLUMNS[0], "new text".getBytes());
      region_incommon.commit(batchUpdate);

      assertCellEquals(region, Bytes.toBytes(t), COLUMNS[0],
        right_now, "new text");
      assertCellEquals(region, Bytes.toBytes(t), COLUMNS[0],
        one_second_ago, "old text");
      
      // Force a flush so store files come into play.
      region_incommon.flushcache();

      assertCellEquals(region, Bytes.toBytes(t), COLUMNS[0], right_now, "new text");
      assertCellEquals(region, Bytes.toBytes(t), COLUMNS[0], one_second_ago, "old text");

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
  
  /**
   * For HADOOP-2443
   * @throws IOException
   */
  public void testGetClosestRowBefore() throws IOException{

    HRegion region = null;
    BatchUpdate batchUpdate = null;
    
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);
     
      // set up some test data
      String t10 = "010";
      String t20 = "020";
      String t30 = "030";
      String t35 = "035";
      String t40 = "040";
      
      batchUpdate = new BatchUpdate(t10);
      batchUpdate.put(COLUMNS[0], "t10 bytes".getBytes());
      region.batchUpdate(batchUpdate, null);
      
      batchUpdate = new BatchUpdate(t20);
      batchUpdate.put(COLUMNS[0], "t20 bytes".getBytes());
      region.batchUpdate(batchUpdate, null);
      
      batchUpdate = new BatchUpdate(t30);
      batchUpdate.put(COLUMNS[0], "t30 bytes".getBytes());
      region.batchUpdate(batchUpdate, null);
      
      batchUpdate = new BatchUpdate(t35);
      batchUpdate.put(COLUMNS[0], "t35 bytes".getBytes());
      region.batchUpdate(batchUpdate, null);
      
      batchUpdate = new BatchUpdate(t35);
      batchUpdate.delete(COLUMNS[0]);
      region.batchUpdate(batchUpdate, null);
      
      batchUpdate = new BatchUpdate(t40);
      batchUpdate.put(COLUMNS[0], "t40 bytes".getBytes());
      region.batchUpdate(batchUpdate, null);
      
      // try finding "015"
      String t15 = "015";
      Map<byte [], Cell> results = 
        region.getClosestRowBefore(Bytes.toBytes(t15), COLUMNS[0]);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t10 bytes");

      // try "020", we should get that row exactly
      results = region.getClosestRowBefore(Bytes.toBytes(t20), COLUMNS[0]);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t20 bytes");
      
      // try "038", should skip deleted "035" and get "030"
      String t38 = "038";
      results = region.getClosestRowBefore(Bytes.toBytes(t38), COLUMNS[0]);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t30 bytes");
      
      // try "050", should get stuff from "040"
      String t50 = "050";
      results = region.getClosestRowBefore(Bytes.toBytes(t50), COLUMNS[0]);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t40 bytes");

      // force a flush
      region.flushcache();

      // try finding "015"
      results = region.getClosestRowBefore(Bytes.toBytes(t15), COLUMNS[0]);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t10 bytes");

      // try "020", we should get that row exactly
      results = region.getClosestRowBefore(Bytes.toBytes(t20), COLUMNS[0]);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t20 bytes");

      // try "038", should skip deleted "035" and get "030"
      results = region.getClosestRowBefore(Bytes.toBytes(t38), COLUMNS[0]);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t30 bytes");

      // try "050", should get stuff from "040"
      results = region.getClosestRowBefore(Bytes.toBytes(t50), COLUMNS[0]);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t40 bytes");
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

  /**
   * For HBASE-40
   * @throws IOException 
   */
  public void testGetFullWithSpecifiedColumns() throws IOException {
    HRegion region = null;
    HRegionIncommon region_incommon = null;
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);
      region_incommon = new HRegionIncommon(region);
      
      // write a row with a bunch of columns
      byte [] row = Bytes.toBytes("some_row");
      BatchUpdate bu = new BatchUpdate(row);
      bu.put(COLUMNS[0], "column 0".getBytes());
      bu.put(COLUMNS[1], "column 1".getBytes());
      bu.put(COLUMNS[2], "column 2".getBytes());
      region.batchUpdate(bu, null);
      
      assertSpecifiedColumns(region, row);
      // try it again with a cache flush to involve the store, not just the 
      // memcache.
      region_incommon.flushcache();
      assertSpecifiedColumns(region, row);
      
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
  
  private void assertSpecifiedColumns(final HRegion region, final byte [] row) 
  throws IOException {
    TreeSet<byte []> all = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    TreeSet<byte []> one = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    TreeSet<byte []> none = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    
    all.add(COLUMNS[0]);
    all.add(COLUMNS[1]);
    all.add(COLUMNS[2]);      
    one.add(COLUMNS[0]);

    // make sure we get all of them with standard getFull
    Map<byte [], Cell> result = region.getFull(row, null, 
      HConstants.LATEST_TIMESTAMP, 1, null);
    assertEquals(new String(result.get(COLUMNS[0]).getValue()), "column 0");
    assertEquals(new String(result.get(COLUMNS[1]).getValue()), "column 1");
    assertEquals(new String(result.get(COLUMNS[2]).getValue()), "column 2");
          
    // try to get just one
    result = region.getFull(row, one, HConstants.LATEST_TIMESTAMP, 1, null);
    assertEquals(new String(result.get(COLUMNS[0]).getValue()), "column 0");
    assertNull(result.get(COLUMNS[1]));                                   
    assertNull(result.get(COLUMNS[2]));                                   
                                                                          
    // try to get all of them (specified)                                 
    result = region.getFull(row, all, HConstants.LATEST_TIMESTAMP, 1, null);       
    assertEquals(new String(result.get(COLUMNS[0]).getValue()), "column 0");
    assertEquals(new String(result.get(COLUMNS[1]).getValue()), "column 1");
    assertEquals(new String(result.get(COLUMNS[2]).getValue()), "column 2");
    
    // try to get none with empty column set
    result = region.getFull(row, none, HConstants.LATEST_TIMESTAMP, 1, null);
    assertNull(result.get(COLUMNS[0]));
    assertNull(result.get(COLUMNS[1]));
    assertNull(result.get(COLUMNS[2]));    
  }  

  private void assertColumnsPresent(final HRegion r, final byte [] row)
  throws IOException {
    Map<byte [], Cell> result = 
      r.getFull(row, null, HConstants.LATEST_TIMESTAMP, 1, null);
    int columnCount = 0;
    for (Map.Entry<byte [], Cell> e: result.entrySet()) {
      columnCount++;
      byte [] column = e.getKey();
      boolean legitColumn = false;
      for (int i = 0; i < COLUMNS.length; i++) {
        // Assert value is same as row.  This is 'nature' of the data added.
        assertTrue(Bytes.equals(row, e.getValue().getValue()));
        if (Bytes.equals(COLUMNS[i], column)) {
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
