/**
 * Copyright 2010 The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test various scanner timeout issues.
 */
public class TestScannerTimeout {

  private final static HBaseTestingUtility
      TEST_UTIL = new HBaseTestingUtility();

  final Log LOG = LogFactory.getLog(getClass());
  private final static byte[] SOME_BYTES = Bytes.toBytes("f");
  private final static byte[] TABLE_NAME = Bytes.toBytes("t");
  private final static int NB_ROWS = 10;
  // Be careful w/ what you set this timer too... it can get in the way of
  // the mini cluster coming up -- the verification in particular.
  private final static int SCANNER_TIMEOUT = 10000;
  private final static int SCANNER_CACHING = 5;

   /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    c.setInt("hbase.regionserver.lease.period", SCANNER_TIMEOUT);
    TEST_UTIL.startMiniCluster(2);
    HTable table = TEST_UTIL.createTable(TABLE_NAME, SOME_BYTES);
     for (int i = 0; i < NB_ROWS; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(SOME_BYTES, SOME_BYTES, SOME_BYTES);
      table.put(put);
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    TEST_UTIL.ensureSomeRegionServersAvailable(2);
  }

  /**
   * Test that we do get a ScannerTimeoutException
   * @throws Exception
   */
  @Test
  public void test2481() throws Exception {
    LOG.info("START ************ test2481");
    Scan scan = new Scan();
    HTable table =
      new HTable(new Configuration(TEST_UTIL.getConfiguration()), TABLE_NAME);
    ResultScanner r = table.getScanner(scan);
    int count = 0;
    try {
      Result res = r.next();
      while (res != null) {
        count++;
        if (count == 5) {
          // Sleep just a bit more to be sure
          Thread.sleep(SCANNER_TIMEOUT+100);
        }
        res = r.next();
      }
    } catch (ScannerTimeoutException e) {
      LOG.info("Got the timeout " + e.getMessage(), e);
      return;
    }
    fail("We should be timing out");
    LOG.info("END ************ test2481");
  }

  /**
   * Test that scanner can continue even if the region server it was reading
   * from failed. Before 2772, it reused the same scanner id.
   * @throws Exception
   */
  @Test
  public void test2772() throws Exception {
    LOG.info("START************ test2772");
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    Scan scan = new Scan();
    // Set a very high timeout, we want to test what happens when a RS
    // fails but the region is recovered before the lease times out.
    // Since the RS is already created, this conf is client-side only for
    // this new table
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(
        HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, SCANNER_TIMEOUT*100);
    HTable higherScanTimeoutTable = new HTable(conf, TABLE_NAME);
    ResultScanner r = higherScanTimeoutTable.getScanner(scan);
    // This takes way less than SCANNER_TIMEOUT*100
    rs.abort("die!");
    Result[] results = r.next(NB_ROWS);
    assertEquals(NB_ROWS, results.length);
    r.close();
    LOG.info("END ************ test2772");

  }
  
  /**
   * Test that scanner won't miss any rows if the region server it was reading
   * from failed. Before 3686, it would skip rows in the scan.
   * @throws Exception
   */
  @Test
  public void test3686a() throws Exception {
    LOG.info("START ************ TEST3686A---1");
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    LOG.info("START ************ TEST3686A---1111");

    Scan scan = new Scan();
    scan.setCaching(SCANNER_CACHING);
    LOG.info("************ TEST3686A");
    MetaReader.fullScanMetaAndPrint(TEST_UTIL.getHBaseCluster().getMaster().getCatalogTracker());
    HTable table = new HTable(TABLE_NAME);
    LOG.info("START ************ TEST3686A---22");

    ResultScanner r = table.getScanner(scan);
    LOG.info("START ************ TEST3686A---33");

    int count = 1;
    r.next();
    LOG.info("START ************ TEST3686A---44");

    // Kill after one call to next(), which got 5 rows.
    rs.abort("die!");
    while(r.next() != null) {
      count ++;
    }
    assertEquals(NB_ROWS, count);
    r.close();
    LOG.info("************ END TEST3686A");
  }
  
  /**
   * Make sure that no rows are lost if the scanner timeout is longer on the
   * client than the server, and the scan times out on the server but not the
   * client.
   * @throws Exception
   */
  @Test
  public void test3686b() throws Exception {
    LOG.info("START ************ test3686b");
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    Scan scan = new Scan();
    scan.setCaching(SCANNER_CACHING);
    // Set a very high timeout, we want to test what happens when a RS
    // fails but the region is recovered before the lease times out.
    // Since the RS is already created, this conf is client-side only for
    // this new table
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(
        HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, SCANNER_TIMEOUT*100);
    HTable higherScanTimeoutTable = new HTable(conf, TABLE_NAME);
    ResultScanner r = higherScanTimeoutTable.getScanner(scan);
    int count = 1;
    r.next();
    // Sleep, allowing the scan to timeout on the server but not on the client.
    Thread.sleep(SCANNER_TIMEOUT+2000);
    while(r.next() != null) {
      count ++;
    }
    assertEquals(NB_ROWS, count);
    r.close();
    LOG.info("END ************ END test3686b");

  }
}