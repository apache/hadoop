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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;

/**
 * Test log deletion as logs are rolled.
 */
public class TestLogRolling extends HBaseTestCase {
  private static final Log LOG = LogFactory.getLog(TestLogRolling.class);
  private MiniDFSCluster dfs;
  private MiniHBaseCluster cluster;
  private HRegionServer server;
  private HLog log;
  private String tableName;
  private byte[] value;
  
  /**
   * constructor
   * @throws Exception
   */
  public TestLogRolling() throws Exception {
    super();
    try {
      this.dfs = null;
      this.cluster = null;
      this.server = null;
      this.log = null;
      this.tableName = null;
      this.value = null;

      // Force a region split after every 768KB
      conf.setLong("hbase.hregion.max.filesize", 768L * 1024L);

      // We roll the log after every 256 writes
      conf.setInt("hbase.regionserver.maxlogentries", 256);

      // For less frequently updated regions flush after every 2 flushes
      conf.setInt("hbase.hregion.memcache.optionalflushcount", 2);

      // We flush the cache after every 8192 bytes
      conf.setInt("hbase.hregion.memcache.flush.size", 8192);

      // Make lease timeout longer, lease checks less frequent
      conf.setInt("hbase.master.lease.period", 10 * 1000);
      conf.setInt("hbase.master.lease.thread.wakefrequency", 5 * 1000);

      // Increase the amount of time between client retries
      conf.setLong("hbase.client.pause", 15 * 1000);

      // Reduce thread wake frequency so that other threads can get
      // a chance to run.
      conf.setInt(HConstants.THREAD_WAKE_FREQUENCY, 2 * 1000);
      
      String className = this.getClass().getName();
      StringBuilder v = new StringBuilder(className);
      while (v.length() < 1000) {
        v.append(className);
      }
      value = v.toString().getBytes(HConstants.UTF8_ENCODING);
      
    } catch (Exception e) {
      LOG.fatal("error in constructor", e);
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    try {
      super.setUp();
      dfs = new MiniDFSCluster(conf, 2, true, (String[]) null);
    } catch (Exception e) {
      StaticTestEnvironment.shutdownDfs(dfs);
      LOG.fatal("error during setUp: ", e);
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void tearDown() throws Exception {
    try {
      super.tearDown();
      if (cluster != null) {                      // shutdown mini HBase cluster
        cluster.shutdown();
      }
      StaticTestEnvironment.shutdownDfs(dfs);
    } catch (Exception e) {
      LOG.fatal("error in tearDown", e);
      throw e;
    }
  }
  
  private void startAndWriteData() throws Exception {
    cluster = new MiniHBaseCluster(conf, 1, dfs, true);
    try {
      Thread.sleep(10 * 1000);                  // Wait for region server to start
    } catch (InterruptedException e) {
      // continue
    }

    this.server = cluster.getRegionThreads().get(0).getRegionServer();
    this.log = server.getLog();

    // When the META table can be opened, the region servers are running
    HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
    
    try {

      // Create the test table and open it
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor(HConstants.COLUMN_FAMILY.toString()));
      HBaseAdmin admin = new HBaseAdmin(conf);
      admin.createTable(desc);
      HTable table = new HTable(conf, new Text(tableName));

      try {
        for (int i = 1; i <= 2048; i++) {    // 2048 writes should cause 8 log rolls
          long lockid =
            table.startUpdate(new Text("row" + String.format("%1$04d", i)));
          table.put(lockid, HConstants.COLUMN_FAMILY, value);
          table.commit(lockid);

          if (i % 256 == 0) {
            // After every 256 writes sleep to let the log roller run

            try {
              Thread.sleep(2000);
            } catch (InterruptedException e) {
              // continue
            }
          }
        }
      } finally {
        table.close();
      }
    } finally {
      meta.close();
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
        new ArrayList<HRegion>(server.getOnlineRegions().values());
      for (HRegion r: regions) {
        r.flushcache();
      }
      
      // Now roll the log
      log.rollWriter();
      
      int count = log.getNumLogFiles();
      LOG.info("after flushing all regions and rolling logs there are " +
          log.getNumLogFiles() + " log files");
      assertTrue(count <= 2);
    } catch (Exception e) {
      LOG.fatal("unexpected exception", e);
      throw e;
    }
  }

}
