/*
 * Copyright 2011 The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A test class to cover aggregate functions, that can be implemented using
 * Coprocessors.
 */
public class TestAggregateProtocol {
  protected static Log myLog = LogFactory.getLog(TestAggregateProtocol.class);

  /**
   * Creating the test infrastructure.
   */
  private static final byte[] TEST_TABLE = Bytes.toBytes("TestTable");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("TestQualifier");
  private static final byte[] TEST_MULTI_CQ = Bytes.toBytes("TestMultiCQ");

  private static byte[] ROW = Bytes.toBytes("testRow");
  private static final int ROWSIZE = 20;
  private static final int rowSeperator1 = 5;
  private static final int rowSeperator2 = 12;
  private static byte[][] ROWS = makeN(ROW, ROWSIZE);

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster = null;
  private static Configuration conf = util.getConfiguration();

  /**
   * A set up method to start the test cluster. AggregateProtocolImpl is
   * registered and will be loaded during region startup.
   * @throws Exception
   */
  @BeforeClass
  public static void setupBeforeClass() throws Exception {

    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.coprocessor.AggregateImplementation");

    util.startMiniCluster(2);
    cluster = util.getMiniHBaseCluster();
    HTable table = util.createTable(TEST_TABLE, TEST_FAMILY);
    util.createMultiRegions(util.getConfiguration(), table, TEST_FAMILY,
        new byte[][] { HConstants.EMPTY_BYTE_ARRAY, ROWS[rowSeperator1],
            ROWS[rowSeperator2] });
    /**
     * The testtable has one CQ which is always populated and one variable CQ
     * for each row rowkey1: CF:CQ CF:CQ1 rowKey2: CF:CQ CF:CQ2
     */
    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      Long l = new Long(i);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(l));
      table.put(put);
      Put p2 = new Put(ROWS[i]);
      p2.add(TEST_FAMILY, Bytes.add(TEST_MULTI_CQ, Bytes.toBytes(l)), Bytes
          .toBytes(l * 10));
      table.put(p2);
    }
  }

  /**
   * Shutting down the cluster
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  /**
   * an infrastructure method to prepare rows for the testtable.
   * @param base
   * @param n
   * @return
   */
  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(i));
    }
    return ret;
  }

  /**
   * **************************** ROW COUNT Test cases *******************
   */

  /**
   * This will test rowcount with a valid range, i.e., a subset of rows. It will
   * be the most common use case.
   * @throws Throwable
   */
  @Test
  public void testRowCountWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[2]);
    scan.setStopRow(ROWS[14]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long rowCount = aClient.rowCount(TEST_TABLE, ci, scan);
    assertEquals(12, rowCount);
  }

  /**
   * This will test the row count on the entire table. Startrow and endrow will
   * be null.
   * @throws Throwable
   */
  @Test
  public void testRowCountAllTable() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long rowCount = aClient.rowCount(TEST_TABLE, ci,
        scan);
    assertEquals(ROWSIZE, rowCount);
  }

  /**
   * This will test the row count with startrow > endrow. The result should be
   * -1.
   * @throws Throwable
   */
  @Test
  public void testRowCountWithInvalidRange1() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[2]);

    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long rowCount = -1;
    try {
      rowCount = aClient.rowCount(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
      myLog.error("Exception thrown in the invalidRange method"
          + e.getStackTrace());
    }
    assertEquals(-1, rowCount);
  }

  /**
   * This will test the row count with startrow = endrow and they will be
   * non-null. The result should be 0, as it assumes a non-get query.
   * @throws Throwable
   */
  @Test
  public void testRowCountWithInvalidRange2() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[5]);

    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long rowCount = -1;
    try {
      rowCount = aClient.rowCount(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
      rowCount = 0;
    }
    assertEquals(0, rowCount);
  }

  /**
   * This should return a 0
   */
  @Test
  public void testRowCountWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long rowCount = -1;
    try {
      rowCount = aClient.rowCount(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
       rowCount = 0;
    }
    assertEquals(0, rowCount);
  }

  @Test
  public void testRowCountWithNullCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long rowCount = aClient.rowCount(TEST_TABLE, ci,
        scan);
    assertEquals(20, rowCount);
  }

  @Test
  public void testRowCountWithPrefixFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    scan.setFilter(f);
    long rowCount = aClient.rowCount(TEST_TABLE, ci,
        scan);
    assertEquals(0, rowCount);
  }

  /**
   * ***************Test cases for Maximum *******************
   */

  /**
   * give max for the entire table.
   * @throws Throwable
   */
  @Test
  public void testMaxWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(19, maximum);
  }

  /**
   * @throws Throwable
   */
  @Test
  public void testMaxWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long max = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(14, max);
  }

  @Test
  public void testMaxWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(190, maximum);
  }

  @Test
  public void testMaxWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long max = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(60, max);
  }

  @Test
  public void testMaxWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Scan scan = new Scan();
    Long max = null;
    try {
      max = aClient.max(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
      max = null;
    }
    assertEquals(null, max);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test
  public void testMaxWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Scan scan = new Scan();
    scan.setStartRow(ROWS[4]);
    scan.setStopRow(ROWS[2]);
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    long max = Long.MIN_VALUE;
    try {
      max = aClient.max(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
      max = 0;
    }
    assertEquals(0, max);// control should go to the catch block
  }

  @Test
  public void testMaxWithInvalidRange2() throws Throwable {
    long max = Long.MIN_VALUE;
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[4]);
    scan.setStopRow(ROWS[4]);
    try {
      AggregationClient aClient = new AggregationClient(conf);
      final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
      max = aClient.max(TEST_TABLE, ci, scan);
    } catch (Exception e) {
      max = 0;
    }
    assertEquals(0, max);// control should go to the catch block
  }

  @Test
  public void testMaxWithFilter() throws Throwable {
    Long max = 0l;
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    scan.setFilter(f);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    max = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(null, max);
  }

  /**
   * **************************Test cases for Minimum ***********************
   */

  /**
   * @throws Throwable
   */
  @Test
  public void testMinWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(HConstants.EMPTY_START_ROW);
    scan.setStopRow(HConstants.EMPTY_END_ROW);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Long min = aClient.min(TEST_TABLE, ci,
        scan);
    assertEquals(0l, min.longValue());
  }

  /**
   * @throws Throwable
   */
  @Test
  public void testMinWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(5, min);
  }

  @Test
  public void testMinWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(HConstants.EMPTY_START_ROW);
    scan.setStopRow(HConstants.EMPTY_END_ROW);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long min = aClient.min(TEST_TABLE, ci,
        scan);
    assertEquals(0, min);
  }

  @Test
  public void testMinWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(6, min);
  }

  @Test
  public void testMinWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Long min = null;
    try {
      min = aClient.min(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, min);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test
  public void testMinWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    Long min = null;
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[4]);
    scan.setStopRow(ROWS[2]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    try {
      min = aClient.min(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, min);// control should go to the catch block
  }

  @Test
  public void testMinWithInvalidRange2() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[6]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Long min = null;
    try {
      min = aClient.min(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, min);// control should go to the catch block
  }

  @Test
  public void testMinWithFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    scan.setFilter(f);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Long min = null;
    min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(null, min);
  }

  /**
   * *************** Test cases for Sum *********************
   */
  /**
   * @throws Throwable
   */
  @Test
  public void testSumWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY,TEST_QUALIFIER);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long sum = aClient.sum(TEST_TABLE, ci,
        scan);
    assertEquals(190, sum);
  }

  /**
   * @throws Throwable
   */
  @Test
  public void testSumWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY,TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(95, sum);
  }

  @Test
  public void testSumWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long sum = aClient.sum(TEST_TABLE, ci,
        scan);
    assertEquals(190 + 1900, sum);
  }

  @Test
  public void testSumWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    long sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(6 + 60, sum);
  }

  @Test
  public void testSumWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Long sum = null;
    try {
      sum = aClient.sum(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, sum);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test
  public void testSumWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[2]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Long sum = null;
    try {
      sum = aClient.sum(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, sum);// control should go to the catch block
  }

  @Test
  public void testSumWithFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setFilter(f);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Long sum = null;
    sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(null, sum);
  }

  /**
   * ****************************** Test Cases for Avg **************
   */
  /**
   * @throws Throwable
   */
  @Test
  public void testAvgWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY,TEST_QUALIFIER);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    double avg = aClient.avg(TEST_TABLE, ci,
        scan);
    assertEquals(9.5, avg, 0);
  }

  /**
   * @throws Throwable
   */
  @Test
  public void testAvgWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY,TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    double avg = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(9.5, avg, 0);
  }

  @Test
  public void testAvgWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    double avg = aClient.avg(TEST_TABLE, ci,
        scan);
    assertEquals(104.5, avg, 0);
  }

  @Test
  public void testAvgWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    double avg = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(6 + 60, avg, 0);
  }

  @Test
  public void testAvgWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Double avg = null;
    try {
      avg = aClient.avg(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, avg);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test
  public void testAvgWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY,TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[1]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Double avg = null;
    try {
      avg = aClient.avg(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, avg);// control should go to the catch block
  }

  @Test
  public void testAvgWithFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY,TEST_QUALIFIER);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    scan.setFilter(f);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Double avg = null;
    avg = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(Double.NaN, avg, 0);
  }

  /**
   * ****************** Test cases for STD **********************
   */
  /**
   * @throws Throwable
   */
  @Test
  public void testStdWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY,TEST_QUALIFIER);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    double std = aClient.std(TEST_TABLE, ci,
        scan);
    assertEquals(5.766, std, 0.05d);
  }

  /**
   * @throws Throwable
   */
  @Test
  public void testStdWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY,TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    double std = aClient.std(TEST_TABLE, ci, scan);
    assertEquals(2.87, std, 0.05d);
  }

  @Test
  public void testStdWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    double std = aClient.std(TEST_TABLE, ci,
        scan);
    assertEquals(63.42, std, 0.05d);
  }

  @Test
  public void testStdWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    double std = aClient.std(TEST_TABLE, ci, scan);
    assertEquals(0, std, 0);
  }

  @Test
  public void testStdWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[17]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Double std = null;
    try {
      std = aClient.std(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, std);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test
  public void testStdWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[1]);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Double std = null;
    try {
      std = aClient.std(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, std);// control should go to the catch block
  }

  @Test
  public void testStdWithFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setFilter(f);
    final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
    Double std = null;
    std = aClient.std(TEST_TABLE, ci, scan);
    assertEquals(Double.NaN, std, 0);
  }
}
