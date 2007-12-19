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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * Tests region server failover when a region server exits both cleanly and
 * when it aborts.
 */
public class TestRegionServerExit extends HBaseClusterTestCase {
  final Log LOG = LogFactory.getLog(this.getClass().getName());
  HTable table;

  /** constructor */
  public TestRegionServerExit() {
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
    Text row = createTableAndAddRow(tableName);
    // Start up a new region server to take over serving of root and meta
    // after we shut down the current meta/root host.
    this.cluster.startRegionServer();
    // Now abort the meta region server and wait for it to go down and come back
    stopOrAbortMetaRegionServer(true);
    // Verify that everything is back up.
    Thread t = startVerificationThread(tableName, row);
    t.start();
    threadDumpingJoin(t);
  }
  
  /**
   * Test abort of region server.
   * @throws IOException
   */
  public void testCleanExit() throws IOException {
    // When the META table can be opened, the region servers are running
    new HTable(this.conf, HConstants.META_TABLE_NAME);
    // Create table and add a row.
    final String tableName = getName();
    Text row = createTableAndAddRow(tableName);
    // Start up a new region server to take over serving of root and meta
    // after we shut down the current meta/root host.
    this.cluster.startRegionServer();
    // Now abort the meta region server and wait for it to go down and come back
    stopOrAbortMetaRegionServer(false);
    // Verify that everything is back up.
    Thread t = startVerificationThread(tableName, row);
    t.start();
    threadDumpingJoin(t);
  }
  
  private Text createTableAndAddRow(final String tableName) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.COLUMN_FAMILY.toString()));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    // put some values in the table
    this.table = new HTable(conf, new Text(tableName));
    final Text row = new Text("row1");
    long lockid = table.startUpdate(row);
    table.put(lockid, HConstants.COLUMN_FAMILY,
        tableName.getBytes(HConstants.UTF8_ENCODING));
    table.commit(lockid);
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
    List<LocalHBaseCluster.RegionServerThread> regionThreads =
      cluster.getRegionThreads();
    
    int server = -1;
    for (int i = 0; i < regionThreads.size() && server == -1; i++) {
      HRegionServer s = regionThreads.get(i).getRegionServer();
      Collection<HRegion> regions = s.getOnlineRegions().values();
      for (HRegion r : regions) {
        if (r.getTableDesc().getName().equals(HConstants.META_TABLE_NAME)) {
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
      final Text row) {
    Runnable runnable = new Runnable() {
      public void run() {
        try {
          // Now try to open a scanner on the meta table. Should stall until
          // meta server comes back up.
          HTable t = new HTable(conf, HConstants.META_TABLE_NAME);
          HScannerInterface s =
            t.obtainScanner(HConstants.COLUMN_FAMILY_ARRAY, new Text());
          s.close();
          
        } catch (IOException e) {
          LOG.fatal("could not re-open meta table because", e);
          fail();
        }
        HScannerInterface scanner = null;
        try {
          // Verify that the client can find the data after the region has moved
          // to a different server
          scanner =
            table.obtainScanner(HConstants.COLUMN_FAMILY_ARRAY, new Text());
          LOG.info("Obtained scanner " + scanner);
          HStoreKey key = new HStoreKey();
          TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
          while (scanner.next(key, results)) {
            assertTrue(key.getRow().equals(row));
            assertEquals(1, results.size());
            byte[] bytes = results.get(HConstants.COLUMN_FAMILY);
            assertNotNull(bytes);
            assertTrue(tableName.equals(new String(bytes,
                HConstants.UTF8_ENCODING)));
          }
          LOG.info("Success!");
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          if (scanner != null) {
            LOG.info("Closing scanner " + scanner);
            try {
              scanner.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      }
    };
    return new Thread(runnable);
  }
}