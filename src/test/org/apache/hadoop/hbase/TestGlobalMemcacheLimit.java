/**
 * Copyright 2008 The Apache Software Foundation
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

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test setting the global memcache size for a region server. When it reaches 
 * this size, any puts should be blocked while one or more forced flushes occurs
 * to bring the memcache size back down. 
 */
public class TestGlobalMemcacheLimit extends HBaseClusterTestCase {
  final byte[] ONE_KB = new byte[1024];

  HTable table1;
  HTable table2;
  HRegionServer server;
  
  long keySize =  COLFAMILY_NAME1.length + 9 + 8;
  long rowSize = keySize + ONE_KB.length;
  
  /**
   * Get our hands into the cluster configuration before the hbase cluster 
   * starts up.
   */
  @Override
  public void preHBaseClusterSetup() {
    // we'll use a 2MB global memcache for testing's sake.
    conf.setInt("hbase.regionserver.globalMemcacheLimit", 2 * 1024 * 1024);
    // low memcache mark will be 1MB
    conf.setInt("hbase.regionserver.globalMemcacheLimitLowMark", 
      1 * 1024 * 1024);
    // make sure we don't do any optional flushes and confuse my tests.
    conf.setInt("hbase.regionserver.optionalcacheflushinterval", 120000);
  }
  
  /**
   * Create a table that we'll use to test.
   */
  @Override
  public void postHBaseClusterSetup() throws IOException {
    HTableDescriptor desc1 = createTableDescriptor("testTable1");
    HTableDescriptor desc2 = createTableDescriptor("testTable2");
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc1);
    admin.createTable(desc2);
    table1 = new HTable(conf, "testTable1");
    table2 = new HTable(conf, "testTable2");    
    server = cluster.getRegionServer(0);    
    
    // there is a META region in play, and those are probably still in
    // the memcache for ROOT. flush it out.
    for (HRegion region : server.getOnlineRegions()) {
      region.flushcache();
    }
    // We used to assert that the memsize here was zero but with the addition
    // of region historian, its no longer true; an entry is added for the
    // flushes run above.
  }
  
  /**
   * Make sure that region server thinks all the memcaches are as big as we were
   * hoping they would be.
   */
  public void testMemcacheSizeAccounting() throws IOException {
    // put some data in each of the two tables
    long dataSize = populate(table1, 500, 0) + populate(table2, 500, 0);
    
    // make sure the region server says it is using as much memory as we think
    // it is.
    // Global cache size is now polluted by region historian data.  We used
    // to be able to do direct compare of global memcache and the data added
    // but not since HBASE-533 went in.  Compare has to be a bit sloppy.
    assertTrue("Global memcache size",
      dataSize <= server.getGlobalMemcacheSize());
  }
  
  /**
   * Test that a put gets blocked and a flush is forced as expected when we 
   * reach the memcache size limit.
   */
  public void testBlocksAndForcesFlush() throws IOException {
    // put some data in each of the two tables
    long startingDataSize = populate(table1, 500, 0) + populate(table2, 500, 0);
    
    // at this point we have 1052000 bytes in memcache. now, we'll keep adding 
    // data to one of the tables until just before the global memcache limit,
    // noting that the globalMemcacheSize keeps growing as expected. then, we'll
    // do another put, causing it to go over the limit. when we look at the
    // globablMemcacheSize now, it should be <= the low limit. 
    long dataNeeded = (2 * 1024 * 1024) - startingDataSize;
    double numRows = (double)dataNeeded / (double)rowSize;
    int preFlushRows = (int)Math.floor(numRows);
  
    long dataAdded = populate(table1, preFlushRows, 500);
    // Global cache size is now polluted by region historian data.  We used
    // to be able to do direct compare of global memcache and the data added
    // but not since HBASE-533 went in.
    long cacheSize = server.getGlobalMemcacheSize();
    assertTrue("Expected memcache size", (dataAdded + startingDataSize) <= cacheSize);
        
    populate(table1, 2, preFlushRows + 500);
    assertTrue("Post-flush memcache size", server.getGlobalMemcacheSize() <= 1024 * 1024);
  }
  
  private long populate(HTable table, int numRows, int startKey)
  throws IOException {
    long total = 0;
    BatchUpdate batchUpdate = null;
    byte [] column = COLFAMILY_NAME1;
    for (int i = startKey; i < startKey + numRows; i++) {
      byte [] key = Bytes.toBytes("row_" + String.format("%1$5d", i));
      total += key.length;
      total += column.length;
      total += 8;
      total += ONE_KB.length;
      batchUpdate = new BatchUpdate(key);
      batchUpdate.put(column, ONE_KB);
      table.commit(batchUpdate);
    }
    return total;
  }
}
