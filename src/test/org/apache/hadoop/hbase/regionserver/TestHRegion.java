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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.regionserver.HRegion.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Basic stand-alone testing of HRegion.
 * 
 * A lot of the meta information for an HRegion now lives inside other
 * HRegions or in the HBaseMaster, so only basic testing is possible.
 */
public class TestHRegion extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestHRegion.class);

  HRegion region = null;
  private final String DIR = "test/build/data/TestHRegion/";
  
  private final int MAX_VERSIONS = 2;

  // Test names
  private final byte[] tableName = Bytes.toBytes("testtable");;
  private final byte[] qual1 = Bytes.toBytes("qual1");
  private final byte[] value1 = Bytes.toBytes("value1");
  private final byte[] value2 = Bytes.toBytes("value2");
  private final byte [] row = Bytes.toBytes("rowA");

  /**
   * @see org.apache.hadoop.hbase.HBaseTestCase#setUp()
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  
  //////////////////////////////////////////////////////////////////////////////
  // New tests that doesn't spin up a mini cluster but rather just test the 
  // individual code pieces in the HRegion. Putting files locally in
  // /tmp/testtable
  //////////////////////////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////////////////////////
  // checkAndPut tests
  //////////////////////////////////////////////////////////////////////////////
  public void testCheckAndPut_WithEmptyRowValue() throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [] qf1  = Bytes.toBytes("qualifier");
    byte [] emptyVal  = new byte[] {};
    byte [] val1  = Bytes.toBytes("value1");
    byte [] val2  = Bytes.toBytes("value2");
    Integer lockId = null;
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, fam1);
    //Putting data in key
    Put put = new Put(row1);
    put.add(fam1, qf1, val1);

    //checkAndPut with correct value
    boolean res = region.checkAndPut(row1, fam1, qf1, emptyVal, put, lockId,
        true);
    assertTrue(res);

    // not empty anymore
    res = region.checkAndPut(row1, fam1, qf1, emptyVal, put, lockId, true);
    assertFalse(res);

    put = new Put(row1);
    put.add(fam1, qf1, val2);
    //checkAndPut with correct value
    res = region.checkAndPut(row1, fam1, qf1, val1, put, lockId, true);
    assertTrue(res);
  }
  
  public void testCheckAndPut_WithWrongValue() throws IOException{
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [] qf1  = Bytes.toBytes("qualifier");
    byte [] val1  = Bytes.toBytes("value1");
    byte [] val2  = Bytes.toBytes("value2");
    Integer lockId = null;

    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, fam1);

    //Putting data in key
    Put put = new Put(row1);
    put.add(fam1, qf1, val1);
    region.put(put);
    
    //checkAndPut with wrong value
    boolean res = region.checkAndPut(row1, fam1, qf1, val2, put, lockId, true);
    assertEquals(false, res);
  }

  public void testCheckAndPut_WithCorrectValue() throws IOException{
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [] qf1  = Bytes.toBytes("qualifier");
    byte [] val1  = Bytes.toBytes("value1");
    Integer lockId = null;

    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, fam1);

    //Putting data in key
    Put put = new Put(row1);
    put.add(fam1, qf1, val1);
    region.put(put);
    
    //checkAndPut with correct value
    boolean res = region.checkAndPut(row1, fam1, qf1, val1, put, lockId, true);
    assertEquals(true, res);
  }
  
  public void testCheckAndPut_ThatPutWasWritten() throws IOException{
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [] fam2 = Bytes.toBytes("fam2");
    byte [] qf1  = Bytes.toBytes("qualifier");
    byte [] val1  = Bytes.toBytes("value1");
    byte [] val2  = Bytes.toBytes("value2");
    Integer lockId = null;

    byte [][] families = {fam1, fam2};
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, families);

    //Putting data in the key to check
    Put put = new Put(row1);
    put.add(fam1, qf1, val1);
    region.put(put);
    
    //Creating put to add
    long ts = System.currentTimeMillis();
    KeyValue kv = new KeyValue(row1, fam2, qf1, ts, KeyValue.Type.Put, val2);
    put = new Put(row1);
    put.add(kv);
    
    //checkAndPut with wrong value
    Store store = region.getStore(fam1);
    int size = store.memstore.kvset.size();
    
    boolean res = region.checkAndPut(row1, fam1, qf1, val1, put, lockId, true);
    assertEquals(true, res);
    size = store.memstore.kvset.size();
    
    Get get = new Get(row1);
    get.addColumn(fam2, qf1);
    KeyValue [] actual = region.get(get, null).raw();
    
    KeyValue [] expected = {kv};
    
    assertEquals(expected.length, actual.length);
    for(int i=0; i<actual.length; i++) {
      assertEquals(expected[i], actual[i]);
    }
    
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Delete tests
  //////////////////////////////////////////////////////////////////////////////
  public void testDelete_CheckFamily() throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [] fam2 = Bytes.toBytes("fam2");
    byte [] fam3 = Bytes.toBytes("fam3");
    byte [] fam4 = Bytes.toBytes("fam4");

    byte[][] families  = {fam1, fam2, fam3};

    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, families);

    List<KeyValue> kvs  = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(row1, fam4, null, null));


    //testing existing family
    byte [] family = fam2;
    try {
      region.delete(family, kvs, true);
    } catch (Exception e) {
      assertTrue("Family " +new String(family)+ " does not exist", false);
    }

    //testing non existing family 
    boolean ok = false; 
    family = fam4;
    try {
      region.delete(family, kvs, true);
    } catch (Exception e) {
      ok = true;
    }
    assertEquals("Family " +new String(family)+ " does exist", true, ok);
  }

  public void testDelete_mixed() throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] fam = Bytes.toBytes("info");
    byte [][] families = {fam};
    String method = this.getName();
    initHRegion(tableName, method, families);

    byte [] row = Bytes.toBytes("table_name");
    // column names
    byte [] serverinfo = Bytes.toBytes("serverinfo");
    byte [] splitA = Bytes.toBytes("splitA");
    byte [] splitB = Bytes.toBytes("splitB");

    // add some data:
    Put put = new Put(row);
    put.add(fam, splitA, Bytes.toBytes("reference_A"));
    region.put(put);

    put = new Put(row);
    put.add(fam, splitB, Bytes.toBytes("reference_B"));
    region.put(put);

    put = new Put(row);
    put.add(fam, serverinfo, Bytes.toBytes("ip_address"));
    region.put(put);

    // ok now delete a split:
    Delete delete = new Delete(row);
    delete.deleteColumns(fam, splitA);
    region.delete(delete, null, true);

    // assert some things:
    Get get = new Get(row).addColumn(fam, serverinfo);
    Result result = region.get(get, null);
    assertEquals(1, result.size());

    get = new Get(row).addColumn(fam, splitA);
    result = region.get(get, null);
    assertEquals(0, result.size());

    get = new Get(row).addColumn(fam, splitB);
    result = region.get(get, null);
    assertEquals(1, result.size());
  }

  public void testScanner_DeleteOneFamilyNotAnother() throws IOException {
    byte [] tableName = Bytes.toBytes("test_table");
    byte [] fam1 = Bytes.toBytes("columnA");
    byte [] fam2 = Bytes.toBytes("columnB");
    initHRegion(tableName, getName(), fam1, fam2);

    byte [] rowA = Bytes.toBytes("rowA");
    byte [] rowB = Bytes.toBytes("rowB");

    byte [] value = Bytes.toBytes("value");

    Delete delete = new Delete(rowA);
    delete.deleteFamily(fam1);

    region.delete(delete, null, true);

    // now create data.
    Put put = new Put(rowA);
    put.add(fam2, null, value);
    region.put(put);

    put = new Put(rowB);
    put.add(fam1, null, value);
    put.add(fam2, null, value);
    region.put(put);

    Scan scan = new Scan();
    scan.addFamily(fam1).addFamily(fam2);
    InternalScanner s = region.getScanner(scan);
    List<KeyValue> results = new ArrayList<KeyValue>();
    s.next(results);
    assertTrue(Bytes.equals(rowA, results.get(0).getRow()));

    results.clear();
    s.next(results);
    assertTrue(Bytes.equals(rowB, results.get(0).getRow()));

  }

  public void testDeleteColumns_PostInsert() throws IOException,
      InterruptedException {
    Delete delete = new Delete(row);
    delete.deleteColumns(fam1, qual1);
    doTestDelete_AndPostInsert(delete);
  }

  public void testDeleteFamily_PostInsert() throws IOException, InterruptedException {
    Delete delete = new Delete(row);
    delete.deleteFamily(fam1);
    doTestDelete_AndPostInsert(delete);
  }

  public void doTestDelete_AndPostInsert(Delete delete)
      throws IOException, InterruptedException {
    initHRegion(tableName, getName(), fam1);
    Put put = new Put(row);
    put.add(fam1, qual1, value1);
    region.put(put);

    Thread.sleep(10);
    
    // now delete the value:
    region.delete(delete, null, true);

    Thread.sleep(10);

    // ok put data:
    put = new Put(row);
    put.add(fam1, qual1, value2);
    region.put(put);

    // ok get:
    Get get = new Get(row);
    get.addColumn(fam1, qual1);

    Result r = region.get(get, null);
    assertEquals(1, r.size());
    assertByteEquals(value2, r.getValue(fam1, qual1));

    // next:
    Scan scan = new Scan(row);
    scan.addColumn(fam1, qual1);
    InternalScanner s = region.getScanner(scan);

    List<KeyValue> results = new ArrayList<KeyValue>();
    assertEquals(false, s.next(results));
    assertEquals(1, results.size());
    KeyValue kv = results.get(0);

    assertByteEquals(value2, kv.getValue());
    assertByteEquals(fam1, kv.getFamily());
    assertByteEquals(qual1, kv.getQualifier());
    assertByteEquals(row, kv.getRow());
  }


  
  public void testDelete_CheckTimestampUpdated()
  throws IOException {
    byte [] row1 = Bytes.toBytes("row1");
    byte [] col1 = Bytes.toBytes("col1");
    byte [] col2 = Bytes.toBytes("col2");
    byte [] col3 = Bytes.toBytes("col3");
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, fam1);
    
    //Building checkerList 
    List<KeyValue> kvs  = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(row1, fam1, col1, null));
    kvs.add(new KeyValue(row1, fam1, col2, null));
    kvs.add(new KeyValue(row1, fam1, col3, null));

    region.delete(fam1, kvs, true);

    // extract the key values out the memstore:
    // This is kinda hacky, but better than nothing...
    long now = System.currentTimeMillis();
    KeyValue firstKv = region.getStore(fam1).memstore.kvset.first();
    assertTrue(firstKv.getTimestamp() <= now);
    now = firstKv.getTimestamp();
    for (KeyValue kv: region.getStore(fam1).memstore.kvset) {
      assertTrue(kv.getTimestamp() <= now);
      now = kv.getTimestamp();
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Get tests
  //////////////////////////////////////////////////////////////////////////////
  public void testGet_FamilyChecker() throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [] fam2 = Bytes.toBytes("False");
    byte [] col1 = Bytes.toBytes("col1");
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, fam1);
    
    Get get = new Get(row1);
    get.addColumn(fam2, col1);
    
    //Test
    try {
      region.get(get, null);
    } catch (NoSuchColumnFamilyException e){
      assertFalse(false);
      return;
    }
    assertFalse(true);
  }

  public void testGet_Basic() throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [] col1 = Bytes.toBytes("col1");
    byte [] col2 = Bytes.toBytes("col2");
    byte [] col3 = Bytes.toBytes("col3");
    byte [] col4 = Bytes.toBytes("col4");
    byte [] col5 = Bytes.toBytes("col5");
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, fam1);
    
    //Add to memstore
    Put put = new Put(row1);
    put.add(fam1, col1, null);
    put.add(fam1, col2, null);
    put.add(fam1, col3, null);
    put.add(fam1, col4, null);
    put.add(fam1, col5, null);
    region.put(put);

    Get get = new Get(row1);
    get.addColumn(fam1, col2);
    get.addColumn(fam1, col4);
    //Expected result
    KeyValue kv1 = new KeyValue(row1, fam1, col2);
    KeyValue kv2 = new KeyValue(row1, fam1, col4);
    KeyValue [] expected = {kv1, kv2};

    //Test
    Result res = region.get(get, null);
    assertEquals(expected.length, res.size());
    for(int i=0; i<res.size(); i++){
      assertEquals(0,
          Bytes.compareTo(expected[i].getRow(), res.raw()[i].getRow()));
      assertEquals(0,
          Bytes.compareTo(expected[i].getFamily(), res.raw()[i].getFamily()));
      assertEquals(0,
          Bytes.compareTo(
              expected[i].getQualifier(), res.raw()[i].getQualifier()));
    }

    // Test using a filter on a Get
    Get g = new Get(row1);
    final int count = 2;
    g.setFilter(new ColumnCountGetFilter(count));
    res = region.get(g, null);
    assertEquals(count, res.size());
  }

  public void testGet_Empty() throws IOException {
    byte [] tableName = Bytes.toBytes("emptytable");
    byte [] row = Bytes.toBytes("row");
    byte [] fam = Bytes.toBytes("fam");
    
    String method = this.getName();
    initHRegion(tableName, method, fam);
    
    Get get = new Get(row);
    get.addFamily(fam);
    Result r = region.get(get, null);
    
    assertTrue(r.isEmpty());
  }
  
  //Test that checked if there was anything special when reading from the ROOT
  //table. To be able to use this test you need to comment the part in
  //HTableDescriptor that checks for '-' and '.'. You also need to remove the
  //s in the beginning of the name.
  public void stestGet_Root() throws IOException {
    //Setting up region
    String method = this.getName();
    initHRegion(HConstants.ROOT_TABLE_NAME, method, HConstants.CATALOG_FAMILY);

    //Add to memstore
    Put put = new Put(HConstants.EMPTY_START_ROW);
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER, null);
    region.put(put);
    
    Get get = new Get(HConstants.EMPTY_START_ROW);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);

    //Expected result
    KeyValue kv1 = new KeyValue(HConstants.EMPTY_START_ROW,
        HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    KeyValue [] expected = {kv1};
    
    //Test from memstore
    Result res = region.get(get, null);
    
    assertEquals(expected.length, res.size());
    for(int i=0; i<res.size(); i++){
      assertEquals(0,
          Bytes.compareTo(expected[i].getRow(), res.raw()[i].getRow()));
      assertEquals(0,
          Bytes.compareTo(expected[i].getFamily(), res.raw()[i].getFamily()));
      assertEquals(0,
          Bytes.compareTo(
              expected[i].getQualifier(), res.raw()[i].getQualifier()));
    }
    
    //flush
    region.flushcache();
    
    //test2
    res = region.get(get, null);
    
    assertEquals(expected.length, res.size());
    for(int i=0; i<res.size(); i++){
      assertEquals(0,
          Bytes.compareTo(expected[i].getRow(), res.raw()[i].getRow()));
      assertEquals(0,
          Bytes.compareTo(expected[i].getFamily(), res.raw()[i].getFamily()));
      assertEquals(0,
          Bytes.compareTo(
              expected[i].getQualifier(), res.raw()[i].getQualifier()));
    }
    
    //Scan
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    InternalScanner s = region.getScanner(scan);
    List<KeyValue> result = new ArrayList<KeyValue>();
    s.next(result);
    
    assertEquals(expected.length, result.size());
    for(int i=0; i<res.size(); i++){
      assertEquals(0,
          Bytes.compareTo(expected[i].getRow(), result.get(i).getRow()));
      assertEquals(0,
          Bytes.compareTo(expected[i].getFamily(), result.get(i).getFamily()));
      assertEquals(0,
          Bytes.compareTo(
              expected[i].getQualifier(), result.get(i).getQualifier()));
    }
  }  
  
  //////////////////////////////////////////////////////////////////////////////
  // Lock test
  ////////////////////////////////////////////////////////////////////////////// 
  public void testLocks() throws IOException{
    byte [] tableName = Bytes.toBytes("testtable");
    byte [][] families = {fam1, fam2, fam3};
    
    HBaseConfiguration hc = initSplit();
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, hc, families);
    
    final int threadCount = 10;
    final int lockCount = 10;
    
    List<Thread>threads = new ArrayList<Thread>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      threads.add(new Thread(Integer.toString(i)) {
        @Override
        public void run() {
          Integer [] lockids = new Integer[lockCount];
          // Get locks.
          for (int i = 0; i < lockCount; i++) {
            try {
              byte [] rowid = Bytes.toBytes(Integer.toString(i));
              lockids[i] = region.obtainRowLock(rowid);
              assertEquals(rowid, region.getRowFromLock(lockids[i]));
              LOG.debug(getName() + " locked " + Bytes.toString(rowid));
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
          LOG.debug(getName() + " set " +
              Integer.toString(lockCount) + " locks");
          
          // Abort outstanding locks.
          for (int i = lockCount - 1; i >= 0; i--) {
            region.releaseRowLock(lockids[i]);
            LOG.debug(getName() + " unlocked " + i);
          }
          LOG.debug(getName() + " released " +
              Integer.toString(lockCount) + " locks");
        }
      });
    }
    
    // Startup all our threads.
    for (Thread t : threads) {
      t.start();
    }
    
    // Now wait around till all are done.
    for (Thread t: threads) {
      while (t.isAlive()) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          // Go around again.
        }
      }
    }
    LOG.info("locks completed.");
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Merge test
  ////////////////////////////////////////////////////////////////////////////// 
  public void testMerge() throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [][] families = {fam1, fam2, fam3};
    
    HBaseConfiguration hc = initSplit();
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, hc, families);
    
    try {
      LOG.info("" + addContent(region, fam3));
      region.flushcache();
      byte [] splitRow = region.compactStores();
      assertNotNull(splitRow);
      LOG.info("SplitRow: " + Bytes.toString(splitRow));
      HRegion [] regions = split(region, splitRow);
      try {
        // Need to open the regions.
        // TODO: Add an 'open' to HRegion... don't do open by constructing
        // instance.
        for (int i = 0; i < regions.length; i++) {
          regions[i] = openClosedRegion(regions[i]);
        }
        Path oldRegionPath = region.getRegionDir();
        long startTime = System.currentTimeMillis();
        HRegion subregions [] = region.splitRegion(splitRow);
        if (subregions != null) {
          LOG.info("Split region elapsed time: "
              + ((System.currentTimeMillis() - startTime) / 1000.0));
          assertEquals("Number of subregions", subregions.length, 2);
          for (int i = 0; i < subregions.length; i++) {
            subregions[i] = openClosedRegion(subregions[i]);
            subregions[i].compactStores();
          }

          // Now merge it back together
          Path oldRegion1 = subregions[0].getRegionDir();
          Path oldRegion2 = subregions[1].getRegionDir();
          startTime = System.currentTimeMillis();
          region = HRegion.mergeAdjacent(subregions[0], subregions[1]);
          LOG.info("Merge regions elapsed time: " +
              ((System.currentTimeMillis() - startTime) / 1000.0));
          fs.delete(oldRegion1, true);
          fs.delete(oldRegion2, true);
          fs.delete(oldRegionPath, true);
        }
        LOG.info("splitAndMerge completed.");
      } finally {
        for (int i = 0; i < regions.length; i++) {
          try {
            regions[i].close();
          } catch (IOException e) {
            // Ignore.
          }
        }
      }
    } finally {
      if (region != null) {
        region.close();
        region.getLog().closeAndDelete();
      }
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Scanner tests
  //////////////////////////////////////////////////////////////////////////////
  public void testGetScanner_WithOkFamilies() throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [] fam2 = Bytes.toBytes("fam2");
    
    byte [][] families = {fam1, fam2};
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, families);
    
    Scan scan = new Scan();
    scan.addFamily(fam1);
    scan.addFamily(fam2);
    try {
      InternalScanner is = region.getScanner(scan);
    } catch (Exception e) {
      assertTrue("Families could not be found in Region", false);
    }
  }
  
  public void testGetScanner_WithNotOkFamilies() throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [] fam2 = Bytes.toBytes("fam2");

    byte [][] families = {fam1};

    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, families);

    Scan scan = new Scan();
    scan.addFamily(fam2);
    boolean ok = false;
    try {
      InternalScanner is = region.getScanner(scan);
    } catch (Exception e) {
      ok = true;
    }
    assertTrue("Families could not be found in Region", ok);
  }
  
  public void testGetScanner_WithNoFamilies() throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [] fam2 = Bytes.toBytes("fam2");
    byte [] fam3 = Bytes.toBytes("fam3");
    byte [] fam4 = Bytes.toBytes("fam4");
    
    byte [][] families = {fam1, fam2, fam3, fam4};
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, families);
    
    //Putting data in Region
    Put put = new Put(row1);
    put.add(fam1, null, null);
    put.add(fam2, null, null);
    put.add(fam3, null, null);
    put.add(fam4, null, null);
    region.put(put);
    
    Scan scan = null;
    InternalScanner is = null;
    
    //Testing to see how many scanners that is produced by getScanner, starting 
    //with known number, 2 - current = 1
    scan = new Scan();
    scan.addFamily(fam2);
    scan.addFamily(fam4);
    is = region.getScanner(scan);
    assertEquals(1, ((RegionScanner)is).getStoreHeap().getHeap().size());
    
    scan = new Scan();
    is = region.getScanner(scan);
    assertEquals(families.length -1, 
        ((RegionScanner)is).getStoreHeap().getHeap().size());
  }

  public void testRegionScanner_Next() throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] row2 = Bytes.toBytes("row2");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [] fam2 = Bytes.toBytes("fam2");
    byte [] fam3 = Bytes.toBytes("fam3");
    byte [] fam4 = Bytes.toBytes("fam4");
    
    byte [][] families = {fam1, fam2, fam3, fam4};
    long ts = System.currentTimeMillis();
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, families);
    
    //Putting data in Region
    Put put = null;
    put = new Put(row1);
    put.add(fam1, null, ts, null);
    put.add(fam2, null, ts, null);
    put.add(fam3, null, ts, null);
    put.add(fam4, null, ts, null);
    region.put(put);

    put = new Put(row2);
    put.add(fam1, null, ts, null);
    put.add(fam2, null, ts, null);
    put.add(fam3, null, ts, null);
    put.add(fam4, null, ts, null);
    region.put(put);
    
    Scan scan = new Scan();
    scan.addFamily(fam2);
    scan.addFamily(fam4);
    InternalScanner is = region.getScanner(scan);
    
    List<KeyValue> res = null;
    
    //Result 1
    List<KeyValue> expected1 = new ArrayList<KeyValue>();
    expected1.add(new KeyValue(row1, fam2, null, ts, KeyValue.Type.Put, null));
    expected1.add(new KeyValue(row1, fam4, null, ts, KeyValue.Type.Put, null));
    
    res = new ArrayList<KeyValue>();
    is.next(res);
    for(int i=0; i<res.size(); i++) {
      assertEquals(expected1.get(i), res.get(i));
    }
    
    //Result 2
    List<KeyValue> expected2 = new ArrayList<KeyValue>();
    expected2.add(new KeyValue(row2, fam2, null, ts, KeyValue.Type.Put, null));
    expected2.add(new KeyValue(row2, fam4, null, ts, KeyValue.Type.Put, null));
    
    res = new ArrayList<KeyValue>();
    is.next(res);
    for(int i=0; i<res.size(); i++) {
      assertEquals(expected2.get(i), res.get(i));
    }
    
  }
  
  public void testScanner_ExplicitColumns_FromMemStore_EnforceVersions() 
  throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] qf1 = Bytes.toBytes("qualifier1");
    byte [] qf2 = Bytes.toBytes("qualifier2");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [][] families = {fam1};
    
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, families);
    
    //Putting data in Region
    Put put = null;
    KeyValue kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
    KeyValue kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
    KeyValue kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);
    
    KeyValue kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
    KeyValue kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
    KeyValue kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);
    
    put = new Put(row1);
    put.add(kv13);
    put.add(kv12);
    put.add(kv11);
    put.add(kv23);
    put.add(kv22);
    put.add(kv21);
    region.put(put);
    
    //Expected
    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(kv13);
    expected.add(kv12);
    
    Scan scan = new Scan(row1);
    scan.addColumn(fam1, qf1);
    scan.setMaxVersions(MAX_VERSIONS);
    List<KeyValue> actual = new ArrayList<KeyValue>();
    InternalScanner scanner = region.getScanner(scan);
    
    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);
    
    //Verify result
    for(int i=0; i<expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }
  
  public void testScanner_ExplicitColumns_FromFilesOnly_EnforceVersions() 
  throws IOException{
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] qf1 = Bytes.toBytes("qualifier1");
    byte [] qf2 = Bytes.toBytes("qualifier2");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [][] families = {fam1};
    
    long ts1 = 1; //System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, families);
    
    //Putting data in Region
    Put put = null;
    KeyValue kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
    KeyValue kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
    KeyValue kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);
    
    KeyValue kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
    KeyValue kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
    KeyValue kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);
    
    put = new Put(row1);
    put.add(kv13);
    put.add(kv12);
    put.add(kv11);
    put.add(kv23);
    put.add(kv22);
    put.add(kv21);
    region.put(put);
    region.flushcache();
    
    //Expected
    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(kv13);
    expected.add(kv12);
    expected.add(kv23);
    expected.add(kv22);
    
    Scan scan = new Scan(row1);
    scan.addColumn(fam1, qf1);
    scan.addColumn(fam1, qf2);
    scan.setMaxVersions(MAX_VERSIONS);
    List<KeyValue> actual = new ArrayList<KeyValue>();
    InternalScanner scanner = region.getScanner(scan);
    
    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);
    
    //Verify result
    for(int i=0; i<expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }
  
  public void testScanner_ExplicitColumns_FromMemStoreAndFiles_EnforceVersions()
  throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [][] families = {fam1};
    byte [] qf1 = Bytes.toBytes("qualifier1");
    byte [] qf2 = Bytes.toBytes("qualifier2");
    
    long ts1 = 1;
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    long ts4 = ts1 + 3;
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, families);
    
    //Putting data in Region
    KeyValue kv14 = new KeyValue(row1, fam1, qf1, ts4, KeyValue.Type.Put, null);
    KeyValue kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
    KeyValue kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
    KeyValue kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);
    
    KeyValue kv24 = new KeyValue(row1, fam1, qf2, ts4, KeyValue.Type.Put, null);
    KeyValue kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
    KeyValue kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
    KeyValue kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);
    
    Put put = null;
    put = new Put(row1);
    put.add(kv14);
    put.add(kv24);
    region.put(put);
    region.flushcache();
    
    put = new Put(row1);
    put.add(kv23);
    put.add(kv13);
    region.put(put);
    region.flushcache();
    
    put = new Put(row1);
    put.add(kv22);
    put.add(kv12);
    region.put(put);
    region.flushcache();
    
    put = new Put(row1);
    put.add(kv21);
    put.add(kv11);
    region.put(put);
    
    //Expected
    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(kv14);
    expected.add(kv13);
    expected.add(kv12);
    expected.add(kv24);
    expected.add(kv23);
    expected.add(kv22);
    
    Scan scan = new Scan(row1);
    scan.addColumn(fam1, qf1);
    scan.addColumn(fam1, qf2);
    int versions = 3;
    scan.setMaxVersions(versions);
    List<KeyValue> actual = new ArrayList<KeyValue>();
    InternalScanner scanner = region.getScanner(scan);
    
    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);
    
    //Verify result
    for(int i=0; i<expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }
  
  public void testScanner_Wildcard_FromMemStore_EnforceVersions() 
  throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] qf1 = Bytes.toBytes("qualifier1");
    byte [] qf2 = Bytes.toBytes("qualifier2");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [][] families = {fam1};
    
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, families);
    
    //Putting data in Region
    Put put = null;
    KeyValue kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
    KeyValue kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
    KeyValue kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);
    
    KeyValue kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
    KeyValue kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
    KeyValue kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);
    
    put = new Put(row1);
    put.add(kv13);
    put.add(kv12);
    put.add(kv11);
    put.add(kv23);
    put.add(kv22);
    put.add(kv21);
    region.put(put);
    
    //Expected
    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(kv13);
    expected.add(kv12);
    expected.add(kv23);
    expected.add(kv22);
    
    Scan scan = new Scan(row1);
    scan.addFamily(fam1);
    scan.setMaxVersions(MAX_VERSIONS);
    List<KeyValue> actual = new ArrayList<KeyValue>();
    InternalScanner scanner = region.getScanner(scan);
    
    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);
    
    //Verify result
    for(int i=0; i<expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }
  
  public void testScanner_Wildcard_FromFilesOnly_EnforceVersions() 
  throws IOException{
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] qf1 = Bytes.toBytes("qualifier1");
    byte [] qf2 = Bytes.toBytes("qualifier2");
    byte [] fam1 = Bytes.toBytes("fam1");

    long ts1 = 1; //System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, fam1);
    
    //Putting data in Region
    Put put = null;
    KeyValue kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
    KeyValue kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
    KeyValue kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);
    
    KeyValue kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
    KeyValue kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
    KeyValue kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);
    
    put = new Put(row1);
    put.add(kv13);
    put.add(kv12);
    put.add(kv11);
    put.add(kv23);
    put.add(kv22);
    put.add(kv21);
    region.put(put);
    region.flushcache();
    
    //Expected
    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(kv13);
    expected.add(kv12);
    expected.add(kv23);
    expected.add(kv22);
    
    Scan scan = new Scan(row1);
    scan.addFamily(fam1);
    scan.setMaxVersions(MAX_VERSIONS);
    List<KeyValue> actual = new ArrayList<KeyValue>();
    InternalScanner scanner = region.getScanner(scan);
    
    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);
    
    //Verify result
    for(int i=0; i<expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  public void testScanner_StopRow1542() throws IOException {
    byte [] tableName = Bytes.toBytes("test_table");
    byte [] family = Bytes.toBytes("testFamily");
    initHRegion(tableName, getName(), family);

    byte [] row1 = Bytes.toBytes("row111");
    byte [] row2 = Bytes.toBytes("row222");
    byte [] row3 = Bytes.toBytes("row333");
    byte [] row4 = Bytes.toBytes("row444");
    byte [] row5 = Bytes.toBytes("row555");

    byte [] col1 = Bytes.toBytes("Pub111");
    byte [] col2 = Bytes.toBytes("Pub222");



    Put put = new Put(row1);
    put.add(family, col1, Bytes.toBytes(10L));
    region.put(put);

    put = new Put(row2);
    put.add(family, col1, Bytes.toBytes(15L));
    region.put(put);

    put = new Put(row3);
    put.add(family, col2, Bytes.toBytes(20L));
    region.put(put);

    put = new Put(row4);
    put.add(family, col2, Bytes.toBytes(30L));
    region.put(put);

    put = new Put(row5);
    put.add(family, col1, Bytes.toBytes(40L));
    region.put(put);

    Scan scan = new Scan(row3, row4);
    scan.setMaxVersions();
    scan.addColumn(family, col1);
    InternalScanner s = region.getScanner(scan);

    List<KeyValue> results = new ArrayList<KeyValue>();
    assertEquals(false, s.next(results));
    assertEquals(0, results.size());



    
  }
  
  public void testScanner_Wildcard_FromMemStoreAndFiles_EnforceVersions()
  throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] row1 = Bytes.toBytes("row1");
    byte [] fam1 = Bytes.toBytes("fam1");
    byte [] qf1 = Bytes.toBytes("qualifier1");
    byte [] qf2 = Bytes.toBytes("quateslifier2");
    
    long ts1 = 1;
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    long ts4 = ts1 + 3;
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, fam1);
    
    //Putting data in Region
    KeyValue kv14 = new KeyValue(row1, fam1, qf1, ts4, KeyValue.Type.Put, null);
    KeyValue kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
    KeyValue kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
    KeyValue kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);
    
    KeyValue kv24 = new KeyValue(row1, fam1, qf2, ts4, KeyValue.Type.Put, null);
    KeyValue kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
    KeyValue kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
    KeyValue kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);
    
    Put put = null;
    put = new Put(row1);
    put.add(kv14);
    put.add(kv24);
    region.put(put);
    region.flushcache();
    
    put = new Put(row1);
    put.add(kv23);
    put.add(kv13);
    region.put(put);
    region.flushcache();
    
    put = new Put(row1);
    put.add(kv22);
    put.add(kv12);
    region.put(put);
    region.flushcache();
    
    put = new Put(row1);
    put.add(kv21);
    put.add(kv11);
    region.put(put);
    
    //Expected
    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(kv14);
    expected.add(kv13);
    expected.add(kv12);
    expected.add(kv24);
    expected.add(kv23);
    expected.add(kv22);
    
    Scan scan = new Scan(row1);
    int versions = 3;
    scan.setMaxVersions(versions);
    List<KeyValue> actual = new ArrayList<KeyValue>();
    InternalScanner scanner = region.getScanner(scan);
    
    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);
    
    //Verify result
    for(int i=0; i<expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Split test
  //////////////////////////////////////////////////////////////////////////////
  /**
   * Splits twice and verifies getting from each of the split regions.
   * @throws Exception
   */
  public void testBasicSplit() throws Exception {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [][] families = {fam1, fam2, fam3};
    
    HBaseConfiguration hc = initSplit();
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, hc, families);
    
    try {
      LOG.info("" + addContent(region, fam3));
      region.flushcache();
      byte [] splitRow = region.compactStores();
      assertNotNull(splitRow);
      LOG.info("SplitRow: " + Bytes.toString(splitRow));
      HRegion [] regions = split(region, splitRow);
      try {
        // Need to open the regions.
        // TODO: Add an 'open' to HRegion... don't do open by constructing
        // instance.
        for (int i = 0; i < regions.length; i++) {
          regions[i] = openClosedRegion(regions[i]);
        }
        // Assert can get rows out of new regions. Should be able to get first
        // row from first region and the midkey from second region.
        assertGet(regions[0], fam3, Bytes.toBytes(START_KEY));
        assertGet(regions[1], fam3, splitRow);
        // Test I can get scanner and that it starts at right place.
        assertScan(regions[0], fam3,
            Bytes.toBytes(START_KEY));
        assertScan(regions[1], fam3, splitRow);
        // Now prove can't split regions that have references.
        for (int i = 0; i < regions.length; i++) {
          // Add so much data to this region, we create a store file that is >
          // than one of our unsplitable references. it will.
          for (int j = 0; j < 2; j++) {
            addContent(regions[i], fam3);
          }
          addContent(regions[i], fam2);
          addContent(regions[i], fam1);
          regions[i].flushcache();
        }

        byte [][] midkeys = new byte [regions.length][];
        // To make regions splitable force compaction.
        for (int i = 0; i < regions.length; i++) {
          midkeys[i] = regions[i].compactStores();
        }

        TreeMap<String, HRegion> sortedMap = new TreeMap<String, HRegion>();
        // Split these two daughter regions so then I'll have 4 regions. Will
        // split because added data above.
        for (int i = 0; i < regions.length; i++) {
          HRegion[] rs = null;
          if (midkeys[i] != null) {
            rs = split(regions[i], midkeys[i]);
            for (int j = 0; j < rs.length; j++) {
              sortedMap.put(Bytes.toString(rs[j].getRegionName()),
                openClosedRegion(rs[j]));
            }
          }
        }
        LOG.info("Made 4 regions");
        // The splits should have been even. Test I can get some arbitrary row
        // out of each.
        int interval = (LAST_CHAR - FIRST_CHAR) / 3;
        byte[] b = Bytes.toBytes(START_KEY);
        for (HRegion r : sortedMap.values()) {
          assertGet(r, fam3, b);
          b[0] += interval;
        }
      } finally {
        for (int i = 0; i < regions.length; i++) {
          try {
            regions[i].close();
          } catch (IOException e) {
            // Ignore.
          }
        }
      }
    } finally {
      if (region != null) {
        region.close();
        region.getLog().closeAndDelete();
      }
    }
  }
  
  public void testSplitRegion() throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [] qualifier = Bytes.toBytes("qualifier");
    HBaseConfiguration hc = initSplit();
    int numRows = 10;
    byte [][] families = {fam1, fam3};
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, hc, families);

    //Put data in region
    int startRow = 100;
    putData(startRow, numRows, qualifier, families);
    int splitRow = startRow + numRows;
    putData(splitRow, numRows, qualifier, families);
    region.flushcache();
    
    HRegion [] regions = null;
    try {
      regions = region.splitRegion(Bytes.toBytes("" + splitRow));
      //Opening the regions returned. 
      for (int i = 0; i < regions.length; i++) {
        regions[i] = openClosedRegion(regions[i]);
      }
      //Verifying that the region has been split
      assertEquals(2, regions.length);
      
      //Verifying that all data is still there and that data is in the right
      //place
      verifyData(regions[0], startRow, numRows, qualifier, families);
      verifyData(regions[1], splitRow, numRows, qualifier, families);
      
    } finally {
      if (region != null) {
        region.close();
        region.getLog().closeAndDelete();
      }
    }
  }
  
  private void putData(int startRow, int numRows, byte [] qf,
      byte [] ...families)
  throws IOException {
    for(int i=startRow; i<startRow+numRows; i++) {
      Put put = new Put(Bytes.toBytes("" + i));
      for(byte [] family : families) {
        put.add(family, qf, null);
      }
      region.put(put);
    }
  }

  private void verifyData(HRegion newReg, int startRow, int numRows, byte [] qf,
      byte [] ... families) 
  throws IOException {
    for(int i=startRow; i<startRow + numRows; i++) {
      byte [] row = Bytes.toBytes("" + i);
      Get get = new Get(row);
      for(byte [] family : families) {
        get.addColumn(family, qf);
      }
      Result result = newReg.get(get, null);
      KeyValue [] raw = result.sorted();
      assertEquals(families.length, result.size());
      for(int j=0; j<families.length; j++) {
        assertEquals(0, Bytes.compareTo(row, raw[j].getRow()));
        assertEquals(0, Bytes.compareTo(families[j], raw[j].getFamily()));
        assertEquals(0, Bytes.compareTo(qf, raw[j].getQualifier()));
      }
    }
  }
  
  
  /**
   * Test for HBASE-810
   * @throws Exception
   */
  public void testScanSplitOnRegion() throws Exception {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [][] families = {fam3};

    HBaseConfiguration hc = initSplit();
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, hc, families);

    try {
      addContent(region, fam3);
      region.flushcache();
      final byte [] midkey = region.compactStores();
      assertNotNull(midkey);
      byte [][] cols = {fam3};
      Scan scan = new Scan();
      scan.addColumns(cols);
      final InternalScanner s = region.getScanner(scan);
      final HRegion regionForThread = region;

      Thread splitThread = new Thread() {
        @Override
        public void run() {
          try {
            split(regionForThread, midkey);
          } catch (IOException e) {
            fail("Unexpected exception " + e);
          } 
        }
      };
      splitThread.start();
      for(int i = 0; i < 6; i++) {
        try {
          Put put = new Put(region.getRegionInfo().getStartKey());
          put.add(fam3, null, Bytes.toBytes("val"));
          region.put(put);
          Thread.sleep(1000);
        }
        catch (InterruptedException e) {
          fail("Unexpected exception " + e);
        }
      }
      s.next(new ArrayList<KeyValue>());
      s.close();
    } catch(UnknownScannerException ex) {
      ex.printStackTrace();
      fail("Got the " + ex);
    } 
  }
  
  
  private void assertGet(final HRegion r, final byte [] family, final byte [] k)
  throws IOException {
    // Now I have k, get values out and assert they are as expected.
    Get get = new Get(k).addFamily(family).setMaxVersions();
    KeyValue [] results = r.get(get, null).raw();
    for (int j = 0; j < results.length; j++) {
      byte [] tmp = results[j].getValue();
      // Row should be equal to value every time.
      assertTrue(Bytes.equals(k, tmp));
    }
  }
  
  /*
   * Assert first value in the passed region is <code>firstValue</code>.
   * @param r
   * @param column
   * @param firstValue
   * @throws IOException
   */
  private void assertScan(final HRegion r, final byte [] column,
      final byte [] firstValue)
  throws IOException {
    byte [][] cols = {column};
    Scan scan = new Scan();
    scan.addColumns(cols);
    InternalScanner s = r.getScanner(scan);
    try {
      List<KeyValue> curVals = new ArrayList<KeyValue>();
      boolean first = true;
      OUTER_LOOP: while(s.next(curVals)) {
        for (KeyValue kv: curVals) {
          byte [] val = kv.getValue();
          byte [] curval = val;
          if (first) {
            first = false;
            assertTrue(Bytes.compareTo(curval, firstValue) == 0);
          } else {
            // Not asserting anything.  Might as well break.
            break OUTER_LOOP;
          }
        }
      }
    } finally {
      s.close();
    }
  }
  
  protected HRegion [] split(final HRegion r, final byte [] splitRow)
  throws IOException {
    // Assert can get mid key from passed region.
    assertGet(r, fam3, splitRow);
    HRegion [] regions = r.splitRegion(splitRow);
    assertEquals(regions.length, 2);
    return regions;
  }
  
  private HBaseConfiguration initSplit() {
    HBaseConfiguration conf = new HBaseConfiguration();
    // Always compact if there is more than one store file.
    conf.setInt("hbase.hstore.compactionThreshold", 2);
    
    // Make lease timeout longer, lease checks less frequent
    conf.setInt("hbase.master.lease.period", 10 * 1000);
    conf.setInt("hbase.master.lease.thread.wakefrequency", 5 * 1000);
    
    conf.setInt("hbase.regionserver.lease.period", 10 * 1000);
    
    // Increase the amount of time between client retries
    conf.setLong("hbase.client.pause", 15 * 1000);

    // This size should make it so we always split using the addContent
    // below.  After adding all data, the first region is 1.3M
    conf.setLong("hbase.hregion.max.filesize", 1024 * 128);
    return conf;
  }  
  
  //////////////////////////////////////////////////////////////////////////////
  // Helpers
  //////////////////////////////////////////////////////////////////////////////
  private HBaseConfiguration initHRegion() {
    HBaseConfiguration conf = new HBaseConfiguration();
    
    conf.set("hbase.hstore.compactionThreshold", "2");
    conf.setLong("hbase.hregion.max.filesize", 65536);
    
    return conf;
  }
  
  private void initHRegion (byte [] tableName, String callingMethod,
      byte[] ... families) throws IOException{
    initHRegion(tableName, callingMethod, new HBaseConfiguration(), families);
  }
  
  private void initHRegion (byte [] tableName, String callingMethod,
      HBaseConfiguration conf, byte [] ... families) throws IOException{
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      htd.addFamily(new HColumnDescriptor(family));
    }
    
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    Path path = new Path(DIR + callingMethod); 
    region = HRegion.createHRegion(info, path, conf);
  }
}
