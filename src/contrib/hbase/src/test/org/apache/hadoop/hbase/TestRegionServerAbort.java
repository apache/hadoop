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

import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Tests region server failover when a region server exits.
 */
public class TestRegionServerAbort extends HBaseClusterTestCase {
  private HTable table;

  /** constructor */
  public TestRegionServerAbort() {
    super();
    conf.setInt("ipc.client.timeout", 5000);            // reduce client timeout
    conf.setInt("ipc.client.connect.max.retries", 5);   // and number of retries
    conf.setInt("hbase.client.retries.number", 2);      // reduce HBase retries
    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger(this.getClass().getPackage().getName()).setLevel(Level.DEBUG);
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  
  /**
   * The test
   * @throws IOException
   */
  public void testRegionServerAbort() throws IOException {
    // When the META table can be opened, the region servers are running
    @SuppressWarnings("unused")
    HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
    // Put something into the meta table.
    String tableName = getName();
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.COLUMN_FAMILY.toString()));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    // put some values in the table
    this.table = new HTable(conf, new Text(tableName));
    Text row = new Text("row1");
    long lockid = table.startUpdate(row);
    table.put(lockid, HConstants.COLUMN_FAMILY,
        tableName.getBytes(HConstants.UTF8_ENCODING));
    table.commit(lockid);
    // Start up a new region server to take over serving of root and meta
    // after we shut down the current meta/root host.
    this.cluster.startRegionServer();
    // Now shutdown the region server and wait for it to go down.
    this.cluster.abortRegionServer(0);
    this.cluster.waitOnRegionServer(0);

    // Verify that the client can find the data after the region has been moved
    // to a different server

    HScannerInterface scanner =
      table.obtainScanner(HConstants.COLUMN_FAMILY_ARRAY, new Text());

    try {
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
      while (scanner.next(key, results)) {
        assertTrue(key.getRow().equals(row));
        assertEquals(1, results.size());
        byte[] bytes = results.get(HConstants.COLUMN_FAMILY);
        assertNotNull(bytes);
        assertTrue(tableName.equals(new String(bytes, HConstants.UTF8_ENCODING)));
      }
      System.out.println("Success!");
    } finally {
      scanner.close();
    }
  }
}
