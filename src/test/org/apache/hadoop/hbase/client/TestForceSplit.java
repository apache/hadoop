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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests forced splitting of HTable
 */
public class TestForceSplit extends HBaseClusterTestCase {
  static final Log LOG = LogFactory.getLog(TestForceSplit.class);
  private static final byte[] tableName = Bytes.toBytes("test");
  private static final byte[] familyName = Bytes.toBytes("a");

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    this.conf.setInt("hbase.io.index.interval", 32);
  }

  /**
   * Tests forcing split from client and having scanners successfully ride over split.
   * @throws Exception 
   * @throws IOException
   */
  @SuppressWarnings("unused") 
  public void testForceSplit() throws Exception {
    // create the test table
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(familyName));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(htd);
    final HTable table = new HTable(conf, tableName);
    byte[] k = new byte[3];
    int rowCount = 0;
    for (byte b1 = 'a'; b1 < 'z'; b1++) {
      for (byte b2 = 'a'; b2 < 'z'; b2++) {
        for (byte b3 = 'a'; b3 < 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          Put put = new Put(k);
          put.add(familyName, new byte[0], k);
          table.put(put);
          rowCount++;
        }
      }
    }

    // get the initial layout (should just be one region)
    Map<HRegionInfo,HServerAddress> m = table.getRegionsInfo();
    System.out.println("Initial regions (" + m.size() + "): " + m);
    assertTrue(m.size() == 1);

    // Verify row count
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    int rows = 0;
    for(Result result : scanner) {
      rows++;
    }
    scanner.close();
    assertEquals(rowCount, rows);
    
    // Have an outstanding scan going on to make sure we can scan over splits.
    scan = new Scan();
    scanner = table.getScanner(scan);
    // Scan first row so we are into first region before split happens.
    scanner.next();

    final AtomicInteger count = new AtomicInteger(0);
    Thread t = new Thread("CheckForSplit") {
      public void run() {
        for (int i = 0; i < 20; i++) {
          try {
            sleep(1000);
          } catch (InterruptedException e) {
            continue;
          }
          // check again    table = new HTable(conf, tableName);
          Map<HRegionInfo, HServerAddress> regions = null;
          try {
            regions = table.getRegionsInfo();
          } catch (IOException e) {
            e.printStackTrace();
          }
          if (regions == null) continue;
          count.set(regions.size());
          if (count.get() >= 2) break;
          LOG.debug("Cycle waiting on split");
        }
      }
    };
    t.start();
    // tell the master to split the table
    admin.split(Bytes.toString(tableName));
    t.join();

    // Verify row count
    rows = 1; // We counted one row above.
    for (Result result : scanner) {
      rows++;
      if (rows > rowCount) {
        scanner.close();
        assertTrue("Scanned more than expected (" + rowCount + ")", false);
      }
    }
    scanner.close();
    assertEquals(rowCount, rows);
  }
}