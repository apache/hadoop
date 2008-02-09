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

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;

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
  
  private static final Text ROW_KEY =
    new Text(HRegionInfo.rootRegionInfo.getRegionName());
  private static final HRegionInfo REGION_INFO = HRegionInfo.rootRegionInfo;
  
  private static final long START_CODE = Long.MAX_VALUE;

  private MiniDFSCluster cluster = null;
  private HRegion r;
  private HRegionIncommon region;

  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR,
      this.cluster.getFileSystem().getHomeDirectory().toString());
    super.setUp();
    
  }
  
  /** Compare the HRegionInfo we read from HBase to what we stored */
  private void validateRegionInfo(byte [] regionBytes) throws IOException {
    HRegionInfo info =
      (HRegionInfo) Writables.getWritable(regionBytes, new HRegionInfo());
    
    assertEquals(REGION_INFO.getRegionId(), info.getRegionId());
    assertEquals(0, info.getStartKey().getLength());
    assertEquals(0, info.getEndKey().getLength());
    assertEquals(0, info.getRegionName().compareTo(REGION_INFO.getRegionName()));
    assertEquals(0, info.getTableDesc().compareTo(REGION_INFO.getTableDesc()));
  }
  
  /** Use a scanner to get the region info and then validate the results */
  private void scan(boolean validateStartcode, String serverName)
      throws IOException {
    
    HScannerInterface scanner = null;
    TreeMap<Text, byte []> results = new TreeMap<Text, byte []>();
    HStoreKey key = new HStoreKey();

    Text[][] scanColumns = {
        COLS,
        EXPLICIT_COLS
    };
    
    for(int i = 0; i < scanColumns.length; i++) {
      try {
        scanner = r.getScanner(scanColumns[i], FIRST_ROW,
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
        HScannerInterface s = scanner;
        scanner = null;
        if(s != null) {
          s.close();
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
    try {
      r = createNewHRegion(REGION_INFO.getTableDesc(), null, null);
      region = new HRegionIncommon(r);
      
      // Write information to the meta table
      
      long lockid = region.startUpdate(ROW_KEY);

      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(byteStream);
      HRegionInfo.rootRegionInfo.write(s);
      region.put(lockid, HConstants.COL_REGIONINFO, byteStream.toByteArray());
      region.commit(lockid, System.currentTimeMillis());

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
      
      lockid = region.startUpdate(ROW_KEY);

      region.put(lockid, HConstants.COL_SERVER, 
        Writables.stringToBytes(address.toString()));

      region.commit(lockid, System.currentTimeMillis());
      
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
      StaticTestEnvironment.shutdownDfs(cluster);
    }
  }
}
