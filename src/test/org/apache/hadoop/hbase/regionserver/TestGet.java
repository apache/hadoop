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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hbase.HBaseTestCase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.BatchUpdate;

/** Test case for get */
public class TestGet extends HBaseTestCase {
  private static final Log LOG = LogFactory.getLog(TestGet.class.getName());
  
  private static final byte [] CONTENTS = Bytes.toBytes("contents:");
  private static final byte [] ROW_KEY =
    HRegionInfo.ROOT_REGIONINFO.getRegionName();
  private static final String SERVER_ADDRESS = "foo.bar.com:1234";


  
  private void verifyGet(final HRegionIncommon r, final String expectedServer)
  throws IOException {
    // This should return a value because there is only one family member
    Cell value = r.get(ROW_KEY, CONTENTS);
    assertNotNull(value);
    
    // This should not return a value because there are multiple family members
    value = r.get(ROW_KEY, HConstants.COLUMN_FAMILY);
    assertNull(value);
    
    // Find out what getFull returns
    Map<byte [], Cell> values = r.getFull(ROW_KEY);
    
    // assertEquals(4, values.keySet().size());
    for (Map.Entry<byte[], Cell> entry : values.entrySet()) {
      byte[] column = entry.getKey();
      Cell cell = entry.getValue();
      if (Bytes.equals(column, HConstants.COL_SERVER)) {
        String server = Writables.cellToString(cell);
        assertEquals(expectedServer, server);
        LOG.info(server);
      }
    }
  }
  
  /** 
   * the test
   * @throws IOException
   */
  public void testGet() throws IOException {
    MiniDFSCluster cluster = null;
    HRegion region = null;

    try {
      
      // Initialization
      
      cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
      // Set the hbase.rootdir to be the home directory in mini dfs.
      this.conf.set(HConstants.HBASE_DIR,
        cluster.getFileSystem().getHomeDirectory().toString());
      
      HTableDescriptor desc = new HTableDescriptor("test");
      desc.addFamily(new HColumnDescriptor(CONTENTS));
      desc.addFamily(new HColumnDescriptor(HConstants.COLUMN_FAMILY));
      
      region = createNewHRegion(desc, null, null);
      HRegionIncommon r = new HRegionIncommon(region);
      
      // Write information to the table

      BatchUpdate batchUpdate = null;
      batchUpdate = new BatchUpdate(ROW_KEY, System.currentTimeMillis());
      batchUpdate.put(CONTENTS, CONTENTS);
      batchUpdate.put(HConstants.COL_REGIONINFO, 
          Writables.getBytes(HRegionInfo.ROOT_REGIONINFO));
      r.commit(batchUpdate);
      
      batchUpdate = new BatchUpdate(ROW_KEY, System.currentTimeMillis());
      batchUpdate.put(HConstants.COL_SERVER, 
        Bytes.toBytes(new HServerAddress(SERVER_ADDRESS).toString()));
      batchUpdate.put(HConstants.COL_STARTCODE, Bytes.toBytes(12345));
      batchUpdate.put(Bytes.toString(HConstants.COLUMN_FAMILY) +
        "region", Bytes.toBytes("region"));
      r.commit(batchUpdate);
      
      // Verify that get works the same from memcache as when reading from disk
      // NOTE dumpRegion won't work here because it only reads from disk.
      
      verifyGet(r, SERVER_ADDRESS);
      
      // Close and re-open region, forcing updates to disk
      
      region.close();
      region = openClosedRegion(region);
      r = new HRegionIncommon(region);
      
      // Read it back
      
      verifyGet(r, SERVER_ADDRESS);
      
      // Update one family member and add a new one
      
      batchUpdate = new BatchUpdate(ROW_KEY, System.currentTimeMillis());
      batchUpdate.put(Bytes.toString(HConstants.COLUMN_FAMILY) + "region",
        "region2".getBytes(HConstants.UTF8_ENCODING));
      String otherServerName = "bar.foo.com:4321";
      batchUpdate.put(HConstants.COL_SERVER, 
        Bytes.toBytes(new HServerAddress(otherServerName).toString()));
      batchUpdate.put(Bytes.toString(HConstants.COLUMN_FAMILY) + "junk",
        "junk".getBytes(HConstants.UTF8_ENCODING));
      r.commit(batchUpdate);

      verifyGet(r, otherServerName);
      
      // Close region and re-open it
      
      region.close();
      region = openClosedRegion(region);
      r = new HRegionIncommon(region);

      // Read it back
      
      verifyGet(r, otherServerName);

    } finally {
      if (region != null) {
        // Close region once and for all
        region.close();
        region.getLog().closeAndDelete();
      }
      if (cluster != null) {
        shutdownDfs(cluster);
      }
    }
  }
}
