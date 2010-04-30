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
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;

/**
 * Tests region server failover when a region server exits both cleanly and
 * when it aborts.
 */
public class DisabledTestRegionServerExit extends HBaseClusterTestCase {
  final Log LOG = LogFactory.getLog(this.getClass().getName());
  HTable table;

  /** constructor */
  public DisabledTestRegionServerExit() {
    super(2);
    conf.setInt("ipc.client.connect.max.retries", 5); // reduce ipc retries
    conf.setInt("ipc.client.timeout", 10000);         // and ipc timeout 
    conf.setInt("hbase.client.pause", 10000);         // increase client timeout
    conf.setInt("hbase.client.retries.number", 10);   // increase HBase retries
  }
  
  /**
   * Test abort of region server.
   * @throws IOException
   */
  public void testAbort() throws IOException {
    // When the META table can be opened, the region servers are running
    new HTable(conf, HConstants.META_TABLE_NAME);
    // Create table and add a row.
    final String tableName = getName();
    byte [] row = createTableAndAddRow(tableName);
    // Start up a new region server to take over serving of root and meta
    // after we shut down the current meta/root host.
    this.cluster.startRegionServer();
    // Now abort the meta region server and wait for it to go down and come back
    stopOrAbortMetaRegionServer(true);
    // Verify that everything is back up.
    LOG.info("Starting up the verification thread for " + getName());
    Thread t = startVerificationThread(tableName, row);
    t.start();
    threadDumpingJoin(t);
  }
  
  /**
   * Test abort of region server.
   * Test is flakey up on hudson.  Needs work.
   * @throws IOException
   */
  public void testCleanExit() throws IOException {
    // When the META table can be opened, the region servers are running
    new HTable(this.conf, HConstants.META_TABLE_NAME);
    // Create table and add a row.
    final String tableName = getName();
    byte [] row = createTableAndAddRow(tableName);
    // Start up a new region server to take over serving of root and meta
    // after we shut down the current meta/root host.
    this.cluster.startRegionServer();
    // Now abort the meta region server and wait for it to go down and come back
    stopOrAbortMetaRegionServer(false);
    // Verify that everything is back up.
    LOG.info("Starting up the verification thread for " + getName());
    Thread t = startVerificationThread(tableName, row);
    t.start();
    threadDumpingJoin(t);
  }
  
  private byte [] createTableAndAddRow(final String tableName)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    // put some values in the table
    this.table = new HTable(conf, tableName);
    byte [] row = Bytes.toBytes("row1");
    Put put = new Put(row);
    put.add(HConstants.CATALOG_FAMILY, null, Bytes.toBytes(tableName));
    table.put(put);
    return row;
  }

  /*
   * Stop the region server serving the meta region and wait for the meta region
   * to get reassigned. This is always the most problematic case.
   * 
   * @param abort set to true if region server should be aborted, if false it
   * is just shut down.
   */
  private void stopOrAbortMetaRegionServer(boolean abort) {
    List<JVMClusterUtil.RegionServerThread> regionThreads =
      cluster.getRegionServerThreads();
    
    int server = -1;
    for (int i = 0; i < regionThreads.size() && server == -1; i++) {
      HRegionServer s = regionThreads.get(i).getRegionServer();
      Collection<HRegion> regions = s.getOnlineRegions();
      for (HRegion r : regions) {
        if (Bytes.equals(r.getTableDesc().getName(),
            HConstants.META_TABLE_NAME)) {
          server = i;
        }
      }
    }
    if (server == -1) {
      LOG.fatal("could not find region server serving meta region");
      fail();
    }
    if (abort) {
      this.cluster.abortRegionServer(server);
      
    } else {
      this.cluster.stopRegionServer(server);
    }
    LOG.info(this.cluster.waitOnRegionServer(server) + " has been " +
        (abort ? "aborted" : "shut down"));
  }
  
  /*
   * Run verification in a thread so I can concurrently run a thread-dumper
   * while we're waiting (because in this test sometimes the meta scanner
   * looks to be be stuck).
   * @param tableName Name of table to find.
   * @param row Row we expect to find.
   * @return Verification thread.  Caller needs to calls start on it.
   */
  private Thread startVerificationThread(final String tableName,
      final byte [] row) {
    Runnable runnable = new Runnable() {
      public void run() {
        try {
          // Now try to open a scanner on the meta table. Should stall until
          // meta server comes back up.
          HTable t = new HTable(conf, HConstants.META_TABLE_NAME);
          Scan scan = new Scan();
          scan.addFamily(HConstants.CATALOG_FAMILY);

          ResultScanner s = t.getScanner(scan);
          s.close();
          
        } catch (IOException e) {
          LOG.fatal("could not re-open meta table because", e);
          fail();
        }
        ResultScanner scanner = null;
        try {
          // Verify that the client can find the data after the region has moved
          // to a different server
          Scan scan = new Scan();
          scan.addFamily(HConstants.CATALOG_FAMILY);

          scanner = table.getScanner(scan);
          LOG.info("Obtained scanner " + scanner);
          for (Result r : scanner) {
            assertTrue(Bytes.equals(r.getRow(), row));
            assertEquals(1, r.size());
            byte[] bytes = r.value();
            assertNotNull(bytes);
            assertTrue(tableName.equals(Bytes.toString(bytes)));
          }
          LOG.info("Success!");
        } catch (Exception e) {
          e.printStackTrace();
          fail();
        } finally {
          if (scanner != null) {
            LOG.info("Closing scanner " + scanner);
            scanner.close();
          }
        }
      }
    };
    return new Thread(runnable);
  }
}