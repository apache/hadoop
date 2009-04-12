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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test the functionality of deleteAll.
 */
public class TestDeleteAll extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestDeleteAll.class);
  
  private final String COLUMN_REGEX = "[a-zA-Z0-9]*:[b|c]?";
  
  private MiniDFSCluster miniHdfs;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    try {
      this.miniHdfs = new MiniDFSCluster(this.conf, 1, true, null);
      // Set the hbase.rootdir to be the home directory in mini dfs.
      this.conf.set(HConstants.HBASE_DIR,
        this.miniHdfs.getFileSystem().getHomeDirectory().toString());
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
      
      // regex test memcache
      makeSureRegexWorks(region, region_incommon, false);
      // regex test hstore
      makeSureRegexWorks(region, region_incommon, true);
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
    byte [] row = Bytes.toBytes("test_row");
    long now = System.currentTimeMillis();
    long past = now - 100;
    long future = now + 100;
    Thread.sleep(100);
    LOG.info("now=" + now + ", past=" + past + ", future=" + future);

    byte [] colA = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "a");
    byte [] colB = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "b");
    byte [] colC = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "c");
    byte [] colD = Bytes.toBytes(Bytes.toString(COLUMNS[0]));

    BatchUpdate batchUpdate = new BatchUpdate(row, now);
    batchUpdate.put(colA, cellData(0, flush).getBytes());
    batchUpdate.put(colB, cellData(0, flush).getBytes());
    batchUpdate.put(colC, cellData(0, flush).getBytes());      
    batchUpdate.put(colD, cellData(0, flush).getBytes());      
    region_incommon.commit(batchUpdate);

    batchUpdate = new BatchUpdate(row, past);
    batchUpdate.put(colA, cellData(1, flush).getBytes());
    batchUpdate.put(colB, cellData(1, flush).getBytes());
    batchUpdate.put(colC, cellData(1, flush).getBytes());      
    batchUpdate.put(colD, cellData(1, flush).getBytes());      
    region_incommon.commit(batchUpdate);
    
    batchUpdate = new BatchUpdate(row, future);
    batchUpdate.put(colA, cellData(2, flush).getBytes());
    batchUpdate.put(colB, cellData(2, flush).getBytes());
    batchUpdate.put(colC, cellData(2, flush).getBytes());      
    batchUpdate.put(colD, cellData(2, flush).getBytes());      
    region_incommon.commit(batchUpdate);

    if (flush) {region_incommon.flushcache();}

    // call delete all at a timestamp, make sure only the most recent stuff is left behind
    region.deleteAll(row, now, null);
    if (flush) {region_incommon.flushcache();}    
    assertCellEquals(region, row, colA, future, cellData(2, flush));
    assertCellEquals(region, row, colA, past, null);
    assertCellEquals(region, row, colA, now, null);
    assertCellEquals(region, row, colD, future, cellData(2, flush));
    assertCellEquals(region, row, colD, past, null);
    assertCellEquals(region, row, colD, now, null);

    // call delete all w/o a timestamp, make sure nothing is left.
    region.deleteAll(row, HConstants.LATEST_TIMESTAMP, null);
    if (flush) {region_incommon.flushcache();}    
    assertCellEquals(region, row, colA, now, null);
    assertCellEquals(region, row, colA, past, null);
    assertCellEquals(region, row, colA, future, null);
    assertCellEquals(region, row, colD, now, null);
    assertCellEquals(region, row, colD, past, null);
    assertCellEquals(region, row, colD, future, null);
    
  }

  private void makeSureRegexWorks(HRegion region, HRegionIncommon region_incommon, 
      boolean flush)
    throws Exception{
      // insert a few versions worth of data for a row
      byte [] row = Bytes.toBytes("test_row");
      long t0 = System.currentTimeMillis();
      long t1 = t0 - 15000;
      long t2 = t1 - 15000;

      byte [] colA = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "a");
      byte [] colB = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "b");
      byte [] colC = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "c");
      byte [] colD = Bytes.toBytes(Bytes.toString(COLUMNS[0]));

      BatchUpdate batchUpdate = new BatchUpdate(row, t0);
      batchUpdate.put(colA, cellData(0, flush).getBytes());
      batchUpdate.put(colB, cellData(0, flush).getBytes());
      batchUpdate.put(colC, cellData(0, flush).getBytes());      
      batchUpdate.put(colD, cellData(0, flush).getBytes());      
      region_incommon.commit(batchUpdate);

      batchUpdate = new BatchUpdate(row, t1);
      batchUpdate.put(colA, cellData(1, flush).getBytes());
      batchUpdate.put(colB, cellData(1, flush).getBytes());
      batchUpdate.put(colC, cellData(1, flush).getBytes());      
      batchUpdate.put(colD, cellData(1, flush).getBytes());      
      region_incommon.commit(batchUpdate);
      
      batchUpdate = new BatchUpdate(row, t2);
      batchUpdate.put(colA, cellData(2, flush).getBytes());
      batchUpdate.put(colB, cellData(2, flush).getBytes());
      batchUpdate.put(colC, cellData(2, flush).getBytes());      
      batchUpdate.put(colD, cellData(2, flush).getBytes());      
      region_incommon.commit(batchUpdate);

      if (flush) {region_incommon.flushcache();}

      // call delete the matching columns at a timestamp, 
      // make sure only the most recent stuff is left behind
      region.deleteAllByRegex(row, COLUMN_REGEX, t1, null);
      if (flush) {region_incommon.flushcache();}    
      assertCellEquals(region, row, colA, t0, cellData(0, flush));
      assertCellEquals(region, row, colA, t1, cellData(1, flush));
      assertCellEquals(region, row, colA, t2, cellData(2, flush));
      assertCellEquals(region, row, colB, t0, cellData(0, flush));
      assertCellEquals(region, row, colB, t1, null);
      assertCellEquals(region, row, colB, t2, null);
      assertCellEquals(region, row, colC, t0, cellData(0, flush));
      assertCellEquals(region, row, colC, t1, null);
      assertCellEquals(region, row, colC, t2, null);
      assertCellEquals(region, row, colD, t0, cellData(0, flush));
      assertCellEquals(region, row, colD, t1, null);
      assertCellEquals(region, row, colD, t2, null);

      // call delete all w/o a timestamp, make sure nothing is left.
      region.deleteAllByRegex(row, COLUMN_REGEX, 
          HConstants.LATEST_TIMESTAMP, null);
      if (flush) {region_incommon.flushcache();}    
      assertCellEquals(region, row, colA, t0, cellData(0, flush));
      assertCellEquals(region, row, colA, t1, cellData(1, flush));
      assertCellEquals(region, row, colA, t2, cellData(2, flush));
      assertCellEquals(region, row, colB, t0, null);
      assertCellEquals(region, row, colB, t1, null);
      assertCellEquals(region, row, colB, t2, null);
      assertCellEquals(region, row, colC, t0, null);
      assertCellEquals(region, row, colC, t1, null);
      assertCellEquals(region, row, colC, t2, null);
      assertCellEquals(region, row, colD, t0, null);
      assertCellEquals(region, row, colD, t1, null);
      assertCellEquals(region, row, colD, t2, null);
      
    }
  
  private String cellData(int tsNum, boolean flush){
    return "t" + tsNum + " data" + (flush ? " - with flush" : "");
  }

  @Override
  protected void tearDown() throws Exception {
    if (this.miniHdfs != null) {
      shutdownDfs(this.miniHdfs);
    }
    super.tearDown();
  }
}
