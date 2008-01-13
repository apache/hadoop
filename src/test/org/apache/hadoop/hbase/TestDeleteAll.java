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

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.commons.logging.*;

/**
 * Test the functionality of deleteAll.
 */
public class TestDeleteAll extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestDeleteAll.class);
  private MiniDFSCluster miniHdfs;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    try {
      this.miniHdfs = new MiniDFSCluster(this.conf, 1, true, null);
    } catch (Exception e) {
      LOG.fatal("error starting MiniDFSCluster", e);
      throw e;
    }
  }
  
  /**
   * Tests for HADOOP-1550.
   * @throws Exception
   */
  public void testDeleteAll() throws Exception {
    HRegion region = null;
    HRegionIncommon region_incommon = null;
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);
      region_incommon = new HRegionIncommon(region);
      
      // test memcache
      makeSureItWorks(region, region_incommon, false);
      // test hstore
      makeSureItWorks(region, region_incommon, true);
      
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
    
  private void makeSureItWorks(HRegion region, HRegionIncommon region_incommon, 
    boolean flush)
  throws Exception{
    // insert a few versions worth of data for a row
    Text row = new Text("test_row");
    long t0 = System.currentTimeMillis();
    long t1 = t0 - 15000;
    long t2 = t1 - 15000;

    Text colA = new Text(COLUMNS[0].toString() + "a");
    Text colB = new Text(COLUMNS[0].toString() + "b");
    Text colC = new Text(COLUMNS[0].toString() + "c");
    Text colD = new Text(COLUMNS[0].toString());
          
    long lock = region_incommon.startUpdate(row);
    region_incommon.put(lock, colA, cellData(0, flush).getBytes());
    region_incommon.put(lock, colB, cellData(0, flush).getBytes());
    region_incommon.put(lock, colC, cellData(0, flush).getBytes());      
    region_incommon.put(lock, colD, cellData(0, flush).getBytes());      
    region_incommon.commit(lock, t0);

    lock = region_incommon.startUpdate(row);
    region_incommon.put(lock, colA, cellData(1, flush).getBytes());
    region_incommon.put(lock, colB, cellData(1, flush).getBytes());
    region_incommon.put(lock, colC, cellData(1, flush).getBytes());      
    region_incommon.put(lock, colD, cellData(1, flush).getBytes());      
    region_incommon.commit(lock, t1);
    
    lock = region_incommon.startUpdate(row);
    region_incommon.put(lock, colA, cellData(2, flush).getBytes());
    region_incommon.put(lock, colB, cellData(2, flush).getBytes());
    region_incommon.put(lock, colC, cellData(2, flush).getBytes());      
    region_incommon.put(lock, colD, cellData(2, flush).getBytes());      
    region_incommon.commit(lock, t2);

    if (flush) {region_incommon.flushcache();}

    // call delete all at a timestamp, make sure only the most recent stuff is left behind
    region.deleteAll(row, t1);
    if (flush) {region_incommon.flushcache();}    
    assertCellValueEquals(region, row, colA, t0, cellData(0, flush));
    assertCellValueEquals(region, row, colA, t1, null);
    assertCellValueEquals(region, row, colA, t2, null);
    assertCellValueEquals(region, row, colD, t0, cellData(0, flush));
    assertCellValueEquals(region, row, colD, t1, null);
    assertCellValueEquals(region, row, colD, t2, null);

    // call delete all w/o a timestamp, make sure nothing is left.
    region.deleteAll(row, HConstants.LATEST_TIMESTAMP);
    if (flush) {region_incommon.flushcache();}    
    assertCellValueEquals(region, row, colA, t0, null);
    assertCellValueEquals(region, row, colA, t1, null);
    assertCellValueEquals(region, row, colA, t2, null);
    assertCellValueEquals(region, row, colD, t0, null);
    assertCellValueEquals(region, row, colD, t1, null);
    assertCellValueEquals(region, row, colD, t2, null);
    
  }
  
  private void assertCellValueEquals(final HRegion region, final Text row,
    final Text column, final long timestamp, final String value)
  throws IOException {
    Map<Text, byte[]> result = region.getFull(row, timestamp);
    byte[] cell_value = result.get(column);
    if(value == null){
      assertEquals(column.toString() + " at timestamp " + timestamp, null, cell_value);
    } else {
      if (cell_value == null) {
        fail(column.toString() + " at timestamp " + timestamp + 
          "\" was expected to be \"" + value + " but was null");
      }
      assertEquals(column.toString() + " at timestamp " 
        + timestamp, value, new String(cell_value));
    }
  }
  
  private String cellData(int tsNum, boolean flush){
    return "t" + tsNum + " data" + (flush ? " - with flush" : "");
  }

  @Override
  protected void tearDown() throws Exception {
    if (this.miniHdfs != null) {
      StaticTestEnvironment.shutdownDfs(this.miniHdfs);
    }
    super.tearDown();
  }
}
