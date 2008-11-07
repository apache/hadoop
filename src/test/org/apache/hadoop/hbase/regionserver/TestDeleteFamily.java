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
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Test the functionality of deleteFamily.
 */
public class TestDeleteFamily extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestDeleteFamily.class);
  private MiniDFSCluster miniHdfs;

  //for family regex deletion test
  protected static final String COLFAMILY_REGEX = "col[a-zA-Z]*1";
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    this.miniHdfs = new MiniDFSCluster(this.conf, 1, true, null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR,
      this.miniHdfs.getFileSystem().getHomeDirectory().toString());
  }
  
  /**
   * Tests for HADOOP-2384.
   * @throws Exception
   */
  public void testDeleteFamily() throws Exception {
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
      // family regex test memcache
      makeSureRegexWorks(region, region_incommon, false);
      // family regex test hstore
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
    long t0 = System.currentTimeMillis();
    long t1 = t0 - 15000;
    long t2 = t1 - 15000;

    byte [] colA = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "a");
    byte [] colB = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "b");
    byte [] colC = Bytes.toBytes(Bytes.toString(COLUMNS[1]) + "c");

    BatchUpdate batchUpdate = null;
    batchUpdate = new BatchUpdate(row, t0);
    batchUpdate.put(colA, cellData(0, flush).getBytes());
    batchUpdate.put(colB, cellData(0, flush).getBytes());
    batchUpdate.put(colC, cellData(0, flush).getBytes());      
    region_incommon.commit(batchUpdate);

    batchUpdate = new BatchUpdate(row, t1);
    batchUpdate.put(colA, cellData(1, flush).getBytes());
    batchUpdate.put(colB, cellData(1, flush).getBytes());
    batchUpdate.put(colC, cellData(1, flush).getBytes());      
    region_incommon.commit(batchUpdate);
    
    batchUpdate = new BatchUpdate(row, t2);
    batchUpdate.put(colA, cellData(2, flush).getBytes());
    batchUpdate.put(colB, cellData(2, flush).getBytes());
    batchUpdate.put(colC, cellData(2, flush).getBytes());      
    region_incommon.commit(batchUpdate);

    if (flush) {region_incommon.flushcache();}

    // call delete family at a timestamp, make sure only the most recent stuff
    // for column c is left behind
    region.deleteFamily(row, COLUMNS[0], t1, null);
    if (flush) {region_incommon.flushcache();}
    // most recent for A,B,C should be fine
    // A,B at older timestamps should be gone
    // C should be fine for older timestamps
    assertCellEquals(region, row, colA, t0, cellData(0, flush));
    assertCellEquals(region, row, colA, t1, null);    
    assertCellEquals(region, row, colA, t2, null);
    assertCellEquals(region, row, colB, t0, cellData(0, flush));
    assertCellEquals(region, row, colB, t1, null);
    assertCellEquals(region, row, colB, t2, null);    
    assertCellEquals(region, row, colC, t0, cellData(0, flush));
    assertCellEquals(region, row, colC, t1, cellData(1, flush));
    assertCellEquals(region, row, colC, t2, cellData(2, flush));        

    // call delete family w/o a timestamp, make sure nothing is left except for
    // column C.
    region.deleteFamily(row, COLUMNS[0], HConstants.LATEST_TIMESTAMP, null);
    if (flush) {region_incommon.flushcache();}
    // A,B for latest timestamp should be gone
    // C should still be fine
    assertCellEquals(region, row, colA, t0, null);
    assertCellEquals(region, row, colB, t0, null);
    assertCellEquals(region, row, colC, t0, cellData(0, flush));
    assertCellEquals(region, row, colC, t1, cellData(1, flush));
    assertCellEquals(region, row, colC, t2, cellData(2, flush));        
    
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
      byte [] colC = Bytes.toBytes(Bytes.toString(COLUMNS[1]) + "c");

      BatchUpdate batchUpdate = null;
      batchUpdate = new BatchUpdate(row, t0);
      batchUpdate.put(colA, cellData(0, flush).getBytes());
      batchUpdate.put(colB, cellData(0, flush).getBytes());
      batchUpdate.put(colC, cellData(0, flush).getBytes());      
      region_incommon.commit(batchUpdate);

      batchUpdate = new BatchUpdate(row, t1);
      batchUpdate.put(colA, cellData(1, flush).getBytes());
      batchUpdate.put(colB, cellData(1, flush).getBytes());
      batchUpdate.put(colC, cellData(1, flush).getBytes());      
      region_incommon.commit(batchUpdate);
      
      batchUpdate = new BatchUpdate(row, t2);
      batchUpdate.put(colA, cellData(2, flush).getBytes());
      batchUpdate.put(colB, cellData(2, flush).getBytes());
      batchUpdate.put(colC, cellData(2, flush).getBytes());      
      region_incommon.commit(batchUpdate);

      if (flush) {region_incommon.flushcache();}

      // call delete family at a timestamp, make sure only the most recent stuff
      // for column c is left behind
      region.deleteFamilyByRegex(row, COLFAMILY_REGEX, t1, null);
      if (flush) {region_incommon.flushcache();}
      // most recent for A,B,C should be fine
      // A,B at older timestamps should be gone
      // C should be fine for older timestamps
      assertCellEquals(region, row, colA, t0, cellData(0, flush));
      assertCellEquals(region, row, colA, t1, null);    
      assertCellEquals(region, row, colA, t2, null);
      assertCellEquals(region, row, colB, t0, cellData(0, flush));
      assertCellEquals(region, row, colB, t1, null);
      assertCellEquals(region, row, colB, t2, null);    
      assertCellEquals(region, row, colC, t0, cellData(0, flush));
      assertCellEquals(region, row, colC, t1, cellData(1, flush));
      assertCellEquals(region, row, colC, t2, cellData(2, flush));        

      // call delete family w/o a timestamp, make sure nothing is left except for
      // column C.
      region.deleteFamilyByRegex(row, COLFAMILY_REGEX, HConstants.LATEST_TIMESTAMP, null);
      if (flush) {region_incommon.flushcache();}
      // A,B for latest timestamp should be gone
      // C should still be fine
      assertCellEquals(region, row, colA, t0, null);
      assertCellEquals(region, row, colB, t0, null);
      assertCellEquals(region, row, colC, t0, cellData(0, flush));
      assertCellEquals(region, row, colC, t1, cellData(1, flush));
      assertCellEquals(region, row, colC, t2, cellData(2, flush));        
      
    }
  
  private String cellData(int tsNum, boolean flush){
    return "t" + tsNum + " data" + (flush ? " - with flush" : "");
  }

  @Override
  protected void tearDown() throws Exception {
    if (this.miniHdfs != null) {
      this.miniHdfs.shutdown();
    }
    super.tearDown();
  }
}
