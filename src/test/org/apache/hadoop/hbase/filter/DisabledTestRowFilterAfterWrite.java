/**
 * Copyright 2008 The Apache Software Foundation
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

package org.apache.hadoop.hbase.filter;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

/** Test regexp filters HBASE-476 */
public class DisabledTestRowFilterAfterWrite extends HBaseClusterTestCase {

  private static final Log LOG = LogFactory.getLog(DisabledTestRowFilterAfterWrite.class.getName());

  static final String TABLE_NAME = "TestTable";
  static final String FAMILY = "C:";
  static final String COLUMN1 = FAMILY + "col1";
  static final byte [] TEXT_COLUMN1 = Bytes.toBytes(COLUMN1);
  static final String COLUMN2 = FAMILY + "col2";
  static final byte [] TEXT_COLUMN2 = Bytes.toBytes(COLUMN2);

  private static final byte [][] columns = {
    TEXT_COLUMN1, TEXT_COLUMN2
  };

  private static final int NUM_ROWS = 10;
  private static final int VALUE_SIZE = 1000;
  private static final byte[] VALUE = new byte[VALUE_SIZE];
  private static final int COL_2_SIZE = 5;
  private static final int KEY_SIZE = 9;
  private static final int NUM_REWRITES = 10;
  static {
    Arrays.fill(VALUE, (byte) 'a');
  }

  /** constructor */
  public DisabledTestRowFilterAfterWrite() {
    super();

    // Make sure the cache gets flushed so we get multiple stores
    conf.setInt("hbase.hregion.memcache.flush.size", (NUM_ROWS * (VALUE_SIZE + COL_2_SIZE + KEY_SIZE)));
    LOG.info("memcach flush : " + conf.get("hbase.hregion.memcache.flush.size"));
    conf.setInt("hbase.regionserver.optionalcacheflushinterval", 100000000);
    // Avoid compaction to keep multiple stores
    conf.setInt("hbase.hstore.compactionThreshold", 10000);

    // Make lease timeout longer, lease checks less frequent
    conf.setInt("hbase.master.lease.period", 10 * 1000);

    // For debugging
    conf.setInt("hbase.regionserver.lease.period", 20 * 60 * 1000);
    conf.setInt("ipc.client.timeout", 20 * 60 * 1000);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Test hbase mapreduce jobs against single region and multi-region tables.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public void testAfterWrite() throws IOException, InterruptedException {
    singleTableTest();
  }

  /*
   * Test against a single region. @throws IOException
   */
  private void singleTableTest() throws IOException, InterruptedException {
    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(FAMILY));

    // Create a table.
    HBaseAdmin admin = new HBaseAdmin(this.conf);
    admin.createTable(desc);

    // insert some data into the test table
    HTable table = new HTable(conf, TABLE_NAME);

    for (int i = 0; i < NUM_ROWS; i++) {
      BatchUpdate b = new BatchUpdate("row_" + String.format("%1$05d", i));

      b.put(TEXT_COLUMN1, VALUE);
      b.put(TEXT_COLUMN2, String.format("%1$05d", i).getBytes());
      table.commit(b);
    }

    // LOG.info("Print table contents using scanner before map/reduce for " + TABLE_NAME);
    // scanTable(TABLE_NAME, false);
    // LOG.info("Print table contents using scanner+filter before map/reduce for " + TABLE_NAME);
    // scanTableWithRowFilter(TABLE_NAME, false);

    // Do some identity write operations on one column of the data.
    for (int n = 0; n < NUM_REWRITES; n++) {
      for (int i = 0; i < NUM_ROWS; i++) {
        BatchUpdate b = new BatchUpdate("row_" + String.format("%1$05d", i));

        b.put(TEXT_COLUMN2, String.format("%1$05d", i).getBytes());
        table.commit(b);
      }
    }

    // Wait for the flush to happen
    LOG.info("Waiting, for flushes to complete");
    Thread.sleep(5 * 1000);
    // Wait for the flush to happen
    LOG.info("Done. No flush should happen after this");

    // Do another round so to populate the mem cache
    for (int i = 0; i < NUM_ROWS; i++) {
      BatchUpdate b = new BatchUpdate("row_" + String.format("%1$05d", i));
      b.put(TEXT_COLUMN2, String.format("%1$05d", i).getBytes());
      table.commit(b);
    }

    LOG.info("Print table contents using scanner after map/reduce for " + TABLE_NAME);
    scanTable(TABLE_NAME, true);
    LOG.info("Print table contents using scanner+filter after map/reduce for " + TABLE_NAME);
    scanTableWithRowFilter(TABLE_NAME, true);
  }

  private void scanTable(final String tableName, final boolean printValues) throws IOException {
    HTable table = new HTable(conf, tableName);

    Scanner scanner = table.getScanner(columns, HConstants.EMPTY_START_ROW);
    int numFound = doScan(scanner, printValues);
    Assert.assertEquals(NUM_ROWS, numFound);
  }

  private void scanTableWithRowFilter(final String tableName, final boolean printValues) throws IOException {
    HTable table = new HTable(conf, tableName);
    Map<byte [], Cell> columnMap = new HashMap<byte [], Cell>();
    columnMap.put(TEXT_COLUMN1,
        new Cell(VALUE, HConstants.LATEST_TIMESTAMP));
    RegExpRowFilter filter = new RegExpRowFilter(null, columnMap);
    Scanner scanner = table.getScanner(columns, HConstants.EMPTY_START_ROW, filter);
    int numFound = doScan(scanner, printValues);
    Assert.assertEquals(NUM_ROWS, numFound);
  }

  private int doScan(final Scanner scanner, final boolean printValues) throws IOException {
    {
      int count = 0;

      try {
        for (RowResult result : scanner) {
          if (printValues) {
            LOG.info("row: " + Bytes.toString(result.getRow()));
            for (Map.Entry<byte [], Cell> e : result.entrySet()) {
              LOG.info(" column: " + e.getKey() + " value: "
                  + new String(e.getValue().getValue(), HConstants.UTF8_ENCODING));
            }
          }
          count++;
        }

      } finally {
        scanner.close();
      }
      return count;
    }
  }
}
