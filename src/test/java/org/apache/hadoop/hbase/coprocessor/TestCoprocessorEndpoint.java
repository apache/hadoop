/*
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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestEndpoint: test cases to verify coprocessor Endpoint
 */
public class TestCoprocessorEndpoint {

  private static final byte[] TEST_TABLE = Bytes.toBytes("TestTable");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("TestQualifier");
  private static byte[] ROW = Bytes.toBytes("testRow");

  private static final int ROWSIZE = 20;
  private static final int rowSeperator1 = 5;
  private static final int rowSeperator2 = 12;
  private static byte[][] ROWS = makeN(ROW, ROWSIZE);

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = util.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.coprocessor.ColumnAggregationEndpoint",
        "org.apache.hadoop.hbase.coprocessor.GenericEndpoint");

    util.startMiniCluster(2);
    cluster = util.getMiniHBaseCluster();

    HTable table = util.createTable(TEST_TABLE, TEST_FAMILY);
    util.createMultiRegions(util.getConfiguration(), table, TEST_FAMILY,
                            new byte[][] { HConstants.EMPTY_BYTE_ARRAY,
                                ROWS[rowSeperator1], ROWS[rowSeperator2] });

    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(i));
      table.put(put);
    }

    // sleep here is an ugly hack to allow region transitions to finish
    Thread.sleep(5000);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testGeneric() throws Throwable {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);
    GenericProtocol protocol = table.coprocessorProxy(GenericProtocol.class,
                                                      Bytes.toBytes("testRow"));
    String workResult1 = protocol.doWork("foo");
    assertEquals("foo", workResult1);
    byte[] workResult2 = protocol.doWork(new byte[]{1});
    assertArrayEquals(new byte[]{1}, workResult2);
    byte workResult3 = protocol.doWork((byte)1);
    assertEquals((byte)1, workResult3);
    char workResult4 = protocol.doWork('c');
    assertEquals('c', workResult4);
    boolean workResult5 = protocol.doWork(true);
    assertEquals(true, workResult5);
    short workResult6 = protocol.doWork((short)1);
    assertEquals((short)1, workResult6);
    int workResult7 = protocol.doWork(5);
    assertEquals(5, workResult7);
    long workResult8 = protocol.doWork(5l);
    assertEquals(5l, workResult8);
    double workResult9 = protocol.doWork(6d);
    assertEquals(6d, workResult9, 0.01);
    float workResult10 = protocol.doWork(6f);
    assertEquals(6f, workResult10, 0.01);
    Text workResult11 = protocol.doWork(new Text("foo"));
    assertEquals(new Text("foo"), workResult11);
  }

  @Test
  public void testAggregation() throws Throwable {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);
    Scan scan;
    Map<byte[], Long> results;

    // scan: for all regions
    results = table
        .coprocessorExec(ColumnAggregationProtocol.class,
                         ROWS[rowSeperator1 - 1], ROWS[rowSeperator2 + 1],
                         new Batch.Call<ColumnAggregationProtocol, Long>() {
                           public Long call(ColumnAggregationProtocol instance)
                               throws IOException {
                             return instance.sum(TEST_FAMILY, TEST_QUALIFIER);
                           }
                         });
    int sumResult = 0;
    int expectedResult = 0;
    for (Map.Entry<byte[], Long> e : results.entrySet()) {
      sumResult += e.getValue();
    }
    for (int i = 0; i < ROWSIZE; i++) {
      expectedResult += i;
    }
    assertEquals("Invalid result", sumResult, expectedResult);

    results.clear();

    // scan: for region 2 and region 3
    results = table
        .coprocessorExec(ColumnAggregationProtocol.class,
                         ROWS[rowSeperator1 + 1], ROWS[rowSeperator2 + 1],
                         new Batch.Call<ColumnAggregationProtocol, Long>() {
                           public Long call(ColumnAggregationProtocol instance)
                               throws IOException {
                             return instance.sum(TEST_FAMILY, TEST_QUALIFIER);
                           }
                         });
    sumResult = 0;
    expectedResult = 0;
    for (Map.Entry<byte[], Long> e : results.entrySet()) {
      sumResult += e.getValue();
    }
    for (int i = rowSeperator1; i < ROWSIZE; i++) {
      expectedResult += i;
    }
    assertEquals("Invalid result", sumResult, expectedResult);
  }

  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(String.format("%02d", i)));
    }
    return ret;
  }
}
