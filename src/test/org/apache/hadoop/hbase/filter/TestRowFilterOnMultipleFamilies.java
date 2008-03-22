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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HScannerInterface;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.io.Text;

/**
 * Test for regexp filters (HBASE-527)
 */
public class TestRowFilterOnMultipleFamilies extends HBaseClusterTestCase {
  private static final Log LOG = LogFactory.getLog(TestRowFilterOnMultipleFamilies.class.getName());

  static final String TABLE_NAME = "TestTable";
  static final String COLUMN1 = "A:col1";
  static final Text TEXT_COLUMN1 = new Text(COLUMN1);
  static final String COLUMN2 = "B:col2";
  static final Text TEXT_COLUMN2 = new Text(COLUMN2);

  private static final Text[] columns = {
    TEXT_COLUMN1, TEXT_COLUMN2
  };

  private static final int NUM_ROWS = 10;
  private static final byte[] VALUE = "HELLO".getBytes();

  /** @throws IOException */
  public void testMultipleFamilies() throws IOException {
    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor("A:"));
    desc.addFamily(new HColumnDescriptor("B:"));

    // Create a table.
    HBaseAdmin admin = new HBaseAdmin(this.conf);
    admin.createTable(desc);

    // insert some data into the test table
    HTable table = new HTable(conf, new Text(TABLE_NAME));

    for (int i = 0; i < NUM_ROWS; i++) {
      BatchUpdate b =
        new BatchUpdate(new Text("row_" + String.format("%1$05d", i)));
      b.put(TEXT_COLUMN1, VALUE);
      b.put(TEXT_COLUMN2, String.format("%1$05d", i).getBytes());
      table.commit(b);
    }

    LOG.info("Print table contents using scanner before map/reduce for " + TABLE_NAME);
    scanTable(TABLE_NAME, true);
    LOG.info("Print table contents using scanner+filter before map/reduce for " + TABLE_NAME);
    scanTableWithRowFilter(TABLE_NAME, true);
  }

  private void scanTable(final String tableName, final boolean printValues) throws IOException {
    HTable table = new HTable(conf, new Text(tableName));

    HScannerInterface scanner = table.obtainScanner(columns, HConstants.EMPTY_START_ROW);
    int numFound = doScan(scanner, printValues);
    Assert.assertEquals(NUM_ROWS, numFound);
  }

  private void scanTableWithRowFilter(final String tableName, final boolean printValues) throws IOException {
    HTable table = new HTable(conf, new Text(tableName));
    Map<Text, byte[]> columnMap = new HashMap<Text, byte[]>();
    columnMap.put(TEXT_COLUMN1, VALUE);
    RegExpRowFilter filter = new RegExpRowFilter(null, columnMap);
    HScannerInterface scanner = table.obtainScanner(columns, HConstants.EMPTY_START_ROW, filter);
    int numFound = doScan(scanner, printValues);
    Assert.assertEquals(NUM_ROWS, numFound);
  }

  private int doScan(final HScannerInterface scanner, final boolean printValues) throws IOException {
    {
      int count = 0;

      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        while (scanner.next(key, results)) {
          if (printValues) {
            LOG.info("row: " + key.getRow());

            for (Map.Entry<Text, byte[]> e : results.entrySet()) {
              LOG.info(" column: " + e.getKey() + " value: "
                  + new String(e.getValue(), HConstants.UTF8_ENCODING));
            }
          }
          Assert.assertEquals(2, results.size());
          count++;
        }

      } finally {
        scanner.close();
      }
      return count;
    }
  }
}
