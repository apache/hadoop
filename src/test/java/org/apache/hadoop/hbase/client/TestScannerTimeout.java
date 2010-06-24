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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
  private final static int SCANNER_TIMEOUT = 6000;
  private static HTable table;

   /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    c.setInt("hbase.regionserver.lease.period", SCANNER_TIMEOUT);
    TEST_UTIL.startMiniCluster(2);
    table = TEST_UTIL.createTable(Bytes.toBytes("t"), SOME_BYTES);
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
    Scan scan = new Scan();
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
  }

  /**
   * Test that scanner can continue even if the region server it was reading
   * from failed. Before 2772, it reused the same scanner id.
   * @throws Exception
   */
  @Test
  public void test2772() throws Exception {
    int rs = TEST_UTIL.getHBaseCluster().getServerWith(
        TEST_UTIL.getHBaseCluster().getRegions(
            TABLE_NAME).get(0).getRegionName());
    Scan scan = new Scan();
    ResultScanner r = table.getScanner(scan);
    // This takes exactly 5 seconds
    TEST_UTIL.getHBaseCluster().getRegionServer(rs).abort("die!");
    Result[] results = r.next(NB_ROWS);
    assertEquals(NB_ROWS, results.length);
    r.close();

  }
}
