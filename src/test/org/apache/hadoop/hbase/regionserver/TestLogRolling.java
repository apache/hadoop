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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseClusterTestCase;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test log deletion as logs are rolled.
 */
public class TestLogRolling extends HBaseClusterTestCase {
  private static final Log LOG = LogFactory.getLog(TestLogRolling.class);
  private HRegionServer server;
  private HLog log;
  private String tableName;
  private byte[] value;
  
  /**
   * constructor
   * @throws Exception
   */
  public TestLogRolling() throws Exception {
    // start one regionserver and a minidfs.
    super();
    try {
      this.server = null;
      this.log = null;
      this.tableName = null;
      this.value = null;
      
      String className = this.getClass().getName();
      StringBuilder v = new StringBuilder(className);
      while (v.length() < 1000) {
        v.append(className);
      }
      value = Bytes.toBytes(v.toString());
      
    } catch (Exception e) {
      LOG.fatal("error in constructor", e);
      throw e;
    }
  }

  // Need to override this setup so we can edit the config before it gets sent
  // to the cluster startup.
  @Override
  protected void preHBaseClusterSetup() {
    // Force a region split after every 768KB
    conf.setLong("hbase.hregion.max.filesize", 768L * 1024L);

    // We roll the log after every 32 writes
    conf.setInt("hbase.regionserver.maxlogentries", 32);

    // For less frequently updated regions flush after every 2 flushes
    conf.setInt("hbase.hregion.memcache.optionalflushcount", 2);

    // We flush the cache after every 8192 bytes
    conf.setInt("hbase.hregion.memcache.flush.size", 8192);

    // Make lease timeout longer, lease checks less frequent
    conf.setInt("hbase.master.lease.period", 10 * 1000);

    // Increase the amount of time between client retries
    conf.setLong("hbase.client.pause", 15 * 1000);

    // Reduce thread wake frequency so that other threads can get
    // a chance to run.
    conf.setInt(HConstants.THREAD_WAKE_FREQUENCY, 2 * 1000);
  }
  
  private void startAndWriteData() throws Exception {
    // When the META table can be opened, the region servers are running
    new HTable(conf, HConstants.META_TABLE_NAME);

    this.server = cluster.getRegionThreads().get(0).getRegionServer();
    this.log = server.getLog();
    
    // Create the test table and open it
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.COLUMN_FAMILY));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    HTable table = new HTable(conf, tableName);

    for (int i = 1; i <= 256; i++) {    // 256 writes should cause 8 log rolls
      BatchUpdate b =
        new BatchUpdate("row" + String.format("%1$04d", i));
      b.put(HConstants.COLUMN_FAMILY, value);
      table.commit(b);

      if (i % 32 == 0) {
        // After every 32 writes sleep to let the log roller run

        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
  }
  
  /**
   * Tests that logs are deleted
   * 
   * @throws Exception
   */
  public void testLogRolling() throws Exception {
    tableName = getName();
    try {
      startAndWriteData();
      LOG.info("after writing there are " + log.getNumLogFiles() + " log files");
      
      // flush all regions
      
      List<HRegion> regions =
        new ArrayList<HRegion>(server.getOnlineRegions());
      for (HRegion r: regions) {
        r.flushcache();
      }
      
      // Now roll the log
      log.rollWriter();
      
      int count = log.getNumLogFiles();
      LOG.info("after flushing all regions and rolling logs there are " +
          log.getNumLogFiles() + " log files");
      assertTrue(("actual count: " + count), count <= 2);
    } catch (Exception e) {
      LOG.fatal("unexpected exception", e);
      throw e;
    }
  }

}
