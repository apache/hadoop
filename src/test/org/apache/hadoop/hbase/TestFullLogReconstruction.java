/**
 * Copyright 2009 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import static org.junit.Assert.assertEquals;

public class TestFullLogReconstruction {

  private final static HBaseTestingUtility
      TEST_UTIL = new HBaseTestingUtility();

  private final static byte[] TABLE_NAME = Bytes.toBytes("tabletest");
  private final static byte[] FAMILY = Bytes.toBytes("family");

  private HBaseConfiguration conf;

    /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().
        setInt("hbase.regionserver.flushlogentries", 1);
    TEST_UTIL.startMiniCluster(2);
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

    conf = TEST_UTIL.getConfiguration();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  /**
   * Test the whole reconstruction loop. Build a table with regions aaa to zzz
   * and load every one of them multiple times with the same date and do a flush
   * at some point. Kill one of the region servers and scan the table. We should
   * see all the rows.
   * @throws Exception
   */
  @Test
  public void testReconstruction() throws Exception {

    TEST_UTIL.createTable(TABLE_NAME, FAMILY);

    HTable table = new HTable(TABLE_NAME);

    TEST_UTIL.createMultiRegions(table, Bytes.toBytes("family"));

    // Load up the table with simple rows and count them
    int initialCount = TEST_UTIL.loadTable(table, FAMILY);
    Scan scan = new Scan();
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count++;
    }
    results.close();

    assertEquals(initialCount, count);

    for(int i = 0; i < 4; i++) {
      TEST_UTIL.loadTable(table, FAMILY);
      if(i == 2) {
        TEST_UTIL.flush();
      }
    }

    TEST_UTIL.expireRegionServerSession(0);

    scan = new Scan();

    results = table.getScanner(scan);

    int newCount = 0;
    for (Result res : results) {
      newCount++;
    }

    assertEquals(count, newCount);

  }

}
