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
    conf.setInt("ipc.client.timeout", 10000);          // reduce client timeout
    conf.setInt("ipc.client.connect.max.retries", 5);  // and number of retries
    conf.setInt("hbase.client.retries.number", 5);     // reduce HBase retries
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
    // Now abort the region server and wait for it to go down.
    this.cluster.abortRegionServer(0);
    LOG.info(this.cluster.waitOnRegionServer(0) + " has been aborted");
    Thread t = startVerificationThread(tableName, row);
    t.start();
    threadDumpingJoin(t);
  }
  
  /**
   * Test abort of region server.
   * @throws IOException
   */
  public void REMOVEtestCleanExit() throws IOException {
    // When the META table can be opened, the region servers are running
    new HTable(this.conf, HConstants.META_TABLE_NAME);
    // Create table and add a row.
    final String tableName = getName();
    Text row = createTableAndAddRow(tableName);
    // Start up a new region server to take over serving of root and meta
    // after we shut down the current meta/root host.
    this.cluster.startRegionServer();
    // Now shutdown the region server and wait for it to go down.
    this.cluster.stopRegionServer(0);
    LOG.info(this.cluster.waitOnRegionServer(0) + " has been shutdown");
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