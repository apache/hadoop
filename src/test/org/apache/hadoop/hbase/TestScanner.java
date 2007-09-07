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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hbase.util.Writables;

/**
 * Test of a long-lived scanner validating as we go.
 */
public class TestScanner extends HBaseTestCase {
  private static final Text FIRST_ROW = new Text();
  private static final Text[] COLS = {
      HConstants.COLUMN_FAMILY
  };
  private static final Text[] EXPLICIT_COLS = {
    HConstants.COL_REGIONINFO,
    HConstants.COL_SERVER,
    HConstants.COL_STARTCODE
  };
  
  private static final Text ROW_KEY = new Text(HGlobals.rootRegionInfo.regionName);
  private static final HRegionInfo REGION_INFO = 
    new HRegionInfo(0L, HGlobals.rootTableDesc, null, null);
  
  private static final long START_CODE = Long.MAX_VALUE;

  private HRegion region;

  /** Compare the HRegionInfo we read from HBase to what we stored */
  private void validateRegionInfo(byte [] regionBytes) throws IOException {
    HRegionInfo info =
      (HRegionInfo) Writables.getWritable(regionBytes, new HRegionInfo());
    
    assertEquals(REGION_INFO.regionId, info.regionId);
    assertEquals(0, info.startKey.getLength());
    assertEquals(0, info.endKey.getLength());
    assertEquals(0, info.regionName.compareTo(REGION_INFO.regionName));
    assertEquals(0, info.tableDesc.compareTo(REGION_INFO.tableDesc));
  }
  
  /** Use a scanner to get the region info and then validate the results */
  private void scan(boolean validateStartcode, String serverName)
      throws IOException {
    
    HInternalScannerInterface scanner = null;
    TreeMap<Text, byte []> results = new TreeMap<Text, byte []>();
    HStoreKey key = new HStoreKey();

    Text[][] scanColumns = {
        COLS,
        EXPLICIT_COLS
    };
    
    for(int i = 0; i < scanColumns.length; i++) {
      try {
        scanner = region.getScanner(scanColumns[i], FIRST_ROW,
            System.currentTimeMillis(), null);
        
        while(scanner.next(key, results)) {
          assertTrue(results.containsKey(HConstants.COL_REGIONINFO));
          byte [] val = results.get(HConstants.COL_REGIONINFO); 
          validateRegionInfo(val);
          if(validateStartcode) {
            assertTrue(results.containsKey(HConstants.COL_STARTCODE));
            val = results.get(HConstants.COL_STARTCODE);
            assertNotNull(val);
            assertFalse(val.length == 0);
            long startCode = Writables.bytesToLong(val);
            assertEquals(START_CODE, startCode);
          }
          
          if(serverName != null) {
            assertTrue(results.containsKey(HConstants.COL_SERVER));
            val = results.get(HConstants.COL_SERVER);
            assertNotNull(val);
            assertFalse(val.length == 0);
            String server = Writables.bytesToString(val);
            assertEquals(0, server.compareTo(serverName));
          }
          results.clear();
        }

      } finally {
        if(scanner != null) {
          scanner.close();
          scanner = null;
        }
      }
    }
  }

  /** Use get to retrieve the HRegionInfo and validate it */
  private void getRegionInfo() throws IOException {
    byte [] bytes = region.get(ROW_KEY, HConstants.COL_REGIONINFO);
    validateRegionInfo(bytes);  
  }
 
  /** The test!
   * @throws IOException
   */
  public void testScanner() throws IOException {
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    
    try {
      
      // Initialization
      
      Configuration conf = new HBaseConfiguration();
      cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
      fs = cluster.getFileSystem();
      Path dir = new Path("/hbase");
      fs.mkdirs(dir);
      
      Path regionDir = HRegion.getRegionDir(dir, REGION_INFO.regionName);
      fs.mkdirs(regionDir);
      
      HLog log = new HLog(fs, new Path(regionDir, "log"), conf);

      region = new HRegion(dir, log, fs, conf, REGION_INFO, null);
      
      // Write information to the meta table
      
      long lockid = region.startUpdate(ROW_KEY);

      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(byteStream);
      HGlobals.rootRegionInfo.write(s);
      region.put(lockid, HConstants.COL_REGIONINFO, byteStream.toByteArray());
      region.commit(lockid, System.currentTimeMillis());

      // What we just committed is in the memcache. Verify that we can get
      // it back both with scanning and get
      
      scan(false, null);
      getRegionInfo();
      
      // Close and re-open
      
      region.close();
      log.rollWriter();
      region = new HRegion(dir, log, fs, conf, REGION_INFO, null);

      // Verify we can get the data back now that it is on disk.
      
      scan(false, null);
      getRegionInfo();
      
      // Store some new information
 
      HServerAddress address = new HServerAddress("foo.bar.com:1234");

      lockid = region.startUpdate(ROW_KEY);

      region.put(lockid, HConstants.COL_SERVER, 
        Writables.stringToBytes(address.toString()));

      region.put(lockid, HConstants.COL_STARTCODE,
          Writables.longToBytes(START_CODE));

      region.commit(lockid, System.currentTimeMillis());
      
      // Validate that we can still get the HRegionInfo, even though it is in
      // an older row on disk and there is a newer row in the memcache
      
      scan(true, address.toString());
      getRegionInfo();
      
      // flush cache

      region.flushcache(false);

      // Validate again
      
      scan(true, address.toString());
      getRegionInfo();

      // Close and reopen
      
      region.close();
      log.rollWriter();
      region = new HRegion(dir, log, fs, conf, REGION_INFO, null);

      // Validate again
      
      scan(true, address.toString());
      getRegionInfo();

      // Now update the information again

      address = new HServerAddress("bar.foo.com:4321");
      
      lockid = region.startUpdate(ROW_KEY);

      region.put(lockid, HConstants.COL_SERVER, 
        Writables.stringToBytes(address.toString()));

      region.commit(lockid, System.currentTimeMillis());
      
      // Validate again
      
      scan(true, address.toString());
      getRegionInfo();

      // flush cache

      region.flushcache(false);

      // Validate again
      
      scan(true, address.toString());
      getRegionInfo();

      // Close and reopen
      
      region.close();
      log.rollWriter();
      region = new HRegion(dir, log, fs, conf, REGION_INFO, null);

      // Validate again
      
      scan(true, address.toString());
      getRegionInfo();
      
      // clean up
      
      region.close();
      log.closeAndDelete();
      
    } finally {
      if(cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
