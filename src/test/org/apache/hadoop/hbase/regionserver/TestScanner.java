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
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Test of a long-lived scanner validating as we go.
 */
public class TestScanner extends HBaseTestCase {
  private final Log LOG = LogFactory.getLog(this.getClass());
  
  private static final byte [] FIRST_ROW =
    HConstants.EMPTY_START_ROW;
  private static final byte [][] COLS = {
      HConstants.COLUMN_FAMILY
  };
  private static final byte [][] EXPLICIT_COLS = {
    HConstants.COL_REGIONINFO,
    HConstants.COL_SERVER,
    HConstants.COL_STARTCODE
  };
  
  static final HTableDescriptor TESTTABLEDESC =
    new HTableDescriptor("testscanner");
  static {
    TESTTABLEDESC.addFamily(new HColumnDescriptor(HConstants.COLUMN_FAMILY,
      10,  // Ten is arbitrary number.  Keep versions to help debuggging.
      Compression.Algorithm.NONE.getName(), false, true, 8 * 1024,
      Integer.MAX_VALUE, HConstants.FOREVER, false));
  }
  /** HRegionInfo for root region */
  public static final HRegionInfo REGION_INFO =
    new HRegionInfo(TESTTABLEDESC, HConstants.EMPTY_BYTE_ARRAY,
    HConstants.EMPTY_BYTE_ARRAY);
  
  private static final byte [] ROW_KEY = REGION_INFO.getRegionName();
  
  private static final long START_CODE = Long.MAX_VALUE;

  private MiniDFSCluster cluster = null;
  private HRegion r;
  private HRegionIncommon region;

  @Override
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR,
      this.cluster.getFileSystem().getHomeDirectory().toString());
    super.setUp();
    
  }

  /** The test!
   * @throws IOException
   */
  public void testScanner() throws IOException {
    try {
      r = createNewHRegion(TESTTABLEDESC, null, null);
      region = new HRegionIncommon(r);
      
      // Write information to the meta table

      BatchUpdate batchUpdate =
        new BatchUpdate(ROW_KEY, System.currentTimeMillis());

      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(byteStream);
      REGION_INFO.write(s);
      batchUpdate.put(HConstants.COL_REGIONINFO, byteStream.toByteArray());
      region.commit(batchUpdate);

      // What we just committed is in the memcache. Verify that we can get
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
 
      HServerAddress address = new HServerAddress("foo.bar.com:1234");

      batchUpdate = new BatchUpdate(ROW_KEY, System.currentTimeMillis());

      batchUpdate.put(HConstants.COL_SERVER,  Bytes.toBytes(address.toString()));

      batchUpdate.put(HConstants.COL_STARTCODE, Bytes.toBytes(START_CODE));

      region.commit(batchUpdate);
      
      // Validate that we can still get the HRegionInfo, even though it is in
      // an older row on disk and there is a newer row in the memcache
      
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

      address = new HServerAddress("bar.foo.com:4321");
      
      batchUpdate = new BatchUpdate(ROW_KEY, System.currentTimeMillis());

      batchUpdate.put(HConstants.COL_SERVER, 
        Bytes.toBytes(address.toString()));

      region.commit(batchUpdate);
      
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
      
      // clean up
      
      r.close();
      r.getLog().closeAndDelete();
      
    } finally {
      shutdownDfs(cluster);
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
    assertEquals(0, info.getTableDesc().compareTo(REGION_INFO.getTableDesc()));
  }
  
  /** Use a scanner to get the region info and then validate the results */
  private void scan(boolean validateStartcode, String serverName)
  throws IOException {  
    InternalScanner scanner = null;
    TreeMap<byte [], Cell> results =
      new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
    HStoreKey key = new HStoreKey();

    byte [][][] scanColumns = {
        COLS,
        EXPLICIT_COLS
    };
    
    for(int i = 0; i < scanColumns.length; i++) {
      try {
        scanner = r.getScanner(scanColumns[i], FIRST_ROW,
            System.currentTimeMillis(), null);
        
        while (scanner.next(key, results)) {
          assertTrue(results.containsKey(HConstants.COL_REGIONINFO));
          byte [] val = results.get(HConstants.COL_REGIONINFO).getValue(); 
          validateRegionInfo(val);
          if(validateStartcode) {
            assertTrue(results.containsKey(HConstants.COL_STARTCODE));
            val = results.get(HConstants.COL_STARTCODE).getValue();
            assertNotNull(val);
            assertFalse(val.length == 0);
            long startCode = Bytes.toLong(val);
            assertEquals(START_CODE, startCode);
          }
          
          if(serverName != null) {
            assertTrue(results.containsKey(HConstants.COL_SERVER));
            val = results.get(HConstants.COL_SERVER).getValue();
            assertNotNull(val);
            assertFalse(val.length == 0);
            String server = Bytes.toString(val);
            assertEquals(0, server.compareTo(serverName));
          }
          results.clear();
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

  /** Use get to retrieve the HRegionInfo and validate it */
  private void getRegionInfo() throws IOException {
    byte [] bytes = region.get(ROW_KEY, HConstants.COL_REGIONINFO).getValue();
    validateRegionInfo(bytes);  
  }

  /**
   * Test basic stop row filter works.
   */
  public void testStopRow() throws Exception {
    byte [] startrow = Bytes.toBytes("bbb");
    byte [] stoprow = Bytes.toBytes("ccc");
    try {
      this.r = createNewHRegion(REGION_INFO.getTableDesc(), null, null);
      addContent(this.r, HConstants.COLUMN_FAMILY);
      InternalScanner s = r.getScanner(HConstants.COLUMN_FAMILY_ARRAY,
        startrow, HConstants.LATEST_TIMESTAMP,
        new WhileMatchRowFilter(new StopRowFilter(stoprow)));
      HStoreKey key = new HStoreKey();
      SortedMap<byte [], Cell> results =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      int count = 0;
      for (boolean first = true; s.next(key, results);) {
        if (first) {
          assertTrue(Bytes.BYTES_COMPARATOR.compare(startrow, key.getRow()) == 0);
          first = false;
        }
        count++;
      }
      assertTrue(Bytes.BYTES_COMPARATOR.compare(stoprow, key.getRow()) > 0);
      // We got something back.
      assertTrue(count > 10);
      s.close();
    } finally {
      this.r.close();
      this.r.getLog().closeAndDelete();
      shutdownDfs(this.cluster);
    }
  }

  /**
   * HBase-910.
   * @throws Exception
   */
  public void testScanAndConcurrentFlush() throws Exception {
    this.r = createNewHRegion(REGION_INFO.getTableDesc(), null, null);
    HRegionIncommon hri = new HRegionIncommon(r);
    try {
      LOG.info("Added: " + 
        addContent(hri, Bytes.toString(HConstants.COL_REGIONINFO)));
      int count = count(hri, -1);
      assertEquals(count, count(hri, 100));
      assertEquals(count, count(hri, 0));
      assertEquals(count, count(hri, count - 1));
    } finally {
      this.r.close();
      this.r.getLog().closeAndDelete();
      shutdownDfs(cluster);
    }
  }
 
  /*
   * @param hri Region
   * @param flushIndex At what row we start the flush.
   * @return Count of rows found.
   * @throws IOException
   */
  private int count(final HRegionIncommon hri, final int flushIndex)
  throws IOException {
    LOG.info("Taking out counting scan");
    ScannerIncommon s = hri.getScanner(EXPLICIT_COLS,
        HConstants.EMPTY_START_ROW, HConstants.LATEST_TIMESTAMP);
    HStoreKey key = new HStoreKey();
    SortedMap<byte [], Cell> values =
      new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
    int count = 0;
    while (s.next(key, values)) {
      count++;
      if (flushIndex == count) {
        LOG.info("Starting flush at flush index " + flushIndex);
        hri.flushcache();
        LOG.info("Finishing flush");
      }
    }
    s.close();
    LOG.info("Found " + count + " items");
    return count;
  }
}
