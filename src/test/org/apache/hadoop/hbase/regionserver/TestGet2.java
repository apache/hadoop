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
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Set;

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HScannerInterface;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.BatchUpdate;

/**
 * {@link TestGet} is a medley of tests of get all done up as a single test.
 * This class 
 */
public class TestGet2 extends HBaseTestCase implements HConstants {
  private MiniDFSCluster miniHdfs;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    this.miniHdfs = new MiniDFSCluster(this.conf, 1, true, null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR,
      this.miniHdfs.getFileSystem().getHomeDirectory().toString());
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
      BatchUpdate batchUpdate = new BatchUpdate(t, one_second_ago);
      batchUpdate.put(COLUMNS[0], "old text".getBytes());
      region_incommon.commit(batchUpdate);
 
      batchUpdate = new BatchUpdate(t, right_now);
      batchUpdate.put(COLUMNS[0], "new text".getBytes());
      region_incommon.commit(batchUpdate);

      assertCellEquals(region, t, COLUMNS[0], right_now, "new text");
      assertCellEquals(region, t, COLUMNS[0], one_second_ago, "old text");
      
      // Force a flush so store files come into play.
      region_incommon.flushcache();

      assertCellEquals(region, t, COLUMNS[0], right_now, "new text");
      assertCellEquals(region, t, COLUMNS[0], one_second_ago, "old text");

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
    HRegionIncommon region_incommon = null;
    BatchUpdate batchUpdate = null;
    
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);
      region_incommon = new HRegionIncommon(region);
     
      // set up some test data
      Text t10 = new Text("010");
      Text t20 = new Text("020");
      Text t30 = new Text("030");
      Text t35 = new Text("035");
      Text t40 = new Text("040");
      
      batchUpdate = new BatchUpdate(t10);
      batchUpdate.put(COLUMNS[0], "t10 bytes".getBytes());
      region.batchUpdate(batchUpdate);
      
      batchUpdate = new BatchUpdate(t20);
      batchUpdate.put(COLUMNS[0], "t20 bytes".getBytes());
      region.batchUpdate(batchUpdate);
      
      batchUpdate = new BatchUpdate(t30);
      batchUpdate.put(COLUMNS[0], "t30 bytes".getBytes());
      region.batchUpdate(batchUpdate);
      
      batchUpdate = new BatchUpdate(t35);
      batchUpdate.put(COLUMNS[0], "t35 bytes".getBytes());
      region.batchUpdate(batchUpdate);
      
      batchUpdate = new BatchUpdate(t35);
      batchUpdate.delete(COLUMNS[0]);
      region.batchUpdate(batchUpdate);
      
      batchUpdate = new BatchUpdate(t40);
      batchUpdate.put(COLUMNS[0], "t40 bytes".getBytes());
      region.batchUpdate(batchUpdate);
      
      // try finding "015"
      Text t15 = new Text("015");
      Map<Text, Cell> results = 
        region.getClosestRowBefore(t15);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t10 bytes");

      // try "020", we should get that row exactly
      results = region.getClosestRowBefore(t20);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t20 bytes");
      
      // try "038", should skip deleted "035" and get "030"
      Text t38 = new Text("038");
      results = region.getClosestRowBefore(t38);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t30 bytes");
      
      // try "050", should get stuff from "040"
      Text t50 = new Text("050");
      results = region.getClosestRowBefore(t50);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t40 bytes");

      // force a flush
      region.flushcache();

      // try finding "015"
      results = region.getClosestRowBefore(t15);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t10 bytes");

      // try "020", we should get that row exactly
      results = region.getClosestRowBefore(t20);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t20 bytes");

      // try "038", should skip deleted "035" and get "030"
      results = region.getClosestRowBefore(t38);
      assertEquals(new String(results.get(COLUMNS[0]).getValue()), "t30 bytes");

      // try "050", should get stuff from "040"
      results = region.getClosestRowBefore(t50);
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
   */
  public void testGetFullWithSpecifiedColumns() throws IOException {
    HRegion region = null;
    HRegionIncommon region_incommon = null;
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);
      region_incommon = new HRegionIncommon(region);
      
      // write a row with a bunch of columns
      Text row = new Text("some_row");
      BatchUpdate bu = new BatchUpdate(row);
      bu.put(COLUMNS[0], "column 0".getBytes());
      bu.put(COLUMNS[1], "column 1".getBytes());
      bu.put(COLUMNS[2], "column 2".getBytes());
      region.batchUpdate(bu);
      
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
  
  private void assertSpecifiedColumns(final HRegion region, final Text row) 
  throws IOException {
    HashSet<Text> all = new HashSet<Text>();
    HashSet<Text> one = new HashSet<Text>();
    HashSet<Text> none = new HashSet<Text>();
    
    all.add(COLUMNS[0]);
    all.add(COLUMNS[1]);
    all.add(COLUMNS[2]);      
    one.add(COLUMNS[0]);

    // make sure we get all of them with standard getFull
    Map<Text, Cell> result = region.getFull(row, null, 
      HConstants.LATEST_TIMESTAMP);
    assertEquals(new String(result.get(COLUMNS[0]).getValue()), "column 0");
    assertEquals(new String(result.get(COLUMNS[1]).getValue()), "column 1");
    assertEquals(new String(result.get(COLUMNS[2]).getValue()), "column 2");
          
    // try to get just one
    result = region.getFull(row, one, HConstants.LATEST_TIMESTAMP);
    assertEquals(new String(result.get(COLUMNS[0]).getValue()), "column 0");
    assertNull(result.get(COLUMNS[1]));                                   
    assertNull(result.get(COLUMNS[2]));                                   
                                                                          
    // try to get all of them (specified)                                 
    result = region.getFull(row, all, HConstants.LATEST_TIMESTAMP);       
    assertEquals(new String(result.get(COLUMNS[0]).getValue()), "column 0");
    assertEquals(new String(result.get(COLUMNS[1]).getValue()), "column 1");
    assertEquals(new String(result.get(COLUMNS[2]).getValue()), "column 2");
    
    // try to get none with empty column set
    result = region.getFull(row, none, HConstants.LATEST_TIMESTAMP);
    assertNull(result.get(COLUMNS[0]));
    assertNull(result.get(COLUMNS[1]));
    assertNull(result.get(COLUMNS[2]));    
  }  
  
  public void testGetFullMultiMapfile() throws IOException {
    HRegion region = null;
    HRegionIncommon region_incommon = null;
    BatchUpdate batchUpdate = null;
    Map<Text, Cell> results = null;
    
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);
      region_incommon = new HRegionIncommon(region);
           
      //
      // Test ordering issue
      //
      Text row = new Text("row1");
     
      // write some data
      batchUpdate = new BatchUpdate(row);
      batchUpdate.put(COLUMNS[0], "olderValue".getBytes());
      region.batchUpdate(batchUpdate);

      // flush
      region.flushcache();
      
      // assert that getFull gives us the older value
      results = region.getFull(row, (Set<Text>)null, LATEST_TIMESTAMP);
      assertEquals("olderValue", new String(results.get(COLUMNS[0]).getValue()));
      
      // write a new value for the cell
      batchUpdate = new BatchUpdate(row);
      batchUpdate.put(COLUMNS[0], "newerValue".getBytes());
      region.batchUpdate(batchUpdate);
      
      // flush
      region.flushcache();
      
      // assert that getFull gives us the later value
      results = region.getFull(row, (Set<Text>)null, LATEST_TIMESTAMP);
      assertEquals("newerValue", new String(results.get(COLUMNS[0]).getValue()));
     
      //
      // Test the delete masking issue
      //
      Text row2 = new Text("row2");
      Text cell1 = new Text(COLUMNS[0].toString() + "a");
      Text cell2 = new Text(COLUMNS[0].toString() + "b");
      Text cell3 = new Text(COLUMNS[0].toString() + "c");
      
      // write some data at two columns
      batchUpdate = new BatchUpdate(row2);
      batchUpdate.put(cell1, "column0 value".getBytes());
      batchUpdate.put(cell2, "column1 value".getBytes());
      region.batchUpdate(batchUpdate);
      
      // flush
      region.flushcache();
      
      // assert i get both columns
      results = region.getFull(row2, (Set<Text>)null, LATEST_TIMESTAMP);
      assertEquals("Should have two columns in the results map", 2, results.size());
      assertEquals("column0 value", new String(results.get(cell1).getValue()));
      assertEquals("column1 value", new String(results.get(cell2).getValue()));
      
      // write a delete for the first column
      batchUpdate = new BatchUpdate(row2);
      batchUpdate.delete(cell1);
      batchUpdate.put(cell2, "column1 new value".getBytes());      
      region.batchUpdate(batchUpdate);
            
      // flush
      region.flushcache(); 
      
      // assert i get the second column only
      results = region.getFull(row2, (Set<Text>)null, LATEST_TIMESTAMP);
      assertEquals("Should have one column in the results map", 1, results.size());
      assertNull("column0 value", results.get(cell1));
      assertEquals("column1 new value", new String(results.get(cell2).getValue()));
      
      //
      // Include a delete and value from the memcache in the mix
      //
      batchUpdate = new BatchUpdate(row2);
      batchUpdate.delete(cell2);      
      batchUpdate.put(cell3, "column2 value!".getBytes());
      region.batchUpdate(batchUpdate);
      
      // assert i get the third column only
      results = region.getFull(row2, (Set<Text>)null, LATEST_TIMESTAMP);
      assertEquals("Should have one column in the results map", 1, results.size());
      assertNull("column0 value", results.get(cell1));
      assertNull("column1 value", results.get(cell2));
      assertEquals("column2 value!", new String(results.get(cell3).getValue()));
      
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
  
  private void assertColumnsPresent(final HRegion r, final Text row)
  throws IOException {
    Map<Text, Cell> result = r.getFull(row, null, HConstants.LATEST_TIMESTAMP);
    int columnCount = 0;
    for (Map.Entry<Text, Cell> e: result.entrySet()) {
      columnCount++;
      String column = e.getKey().toString();
      boolean legitColumn = false;
      for (int i = 0; i < COLUMNS.length; i++) {
        // Assert value is same as row.  This is 'nature' of the data added.
        assertTrue(row.equals(new Text(e.getValue().getValue())));
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
