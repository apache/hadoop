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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

/** test the scanner API at all levels */
public class TestScannerAPI extends HBaseClusterTestCase {
  private final byte [][] columns = Bytes.toByteArrays(new String[] {
    "a:", "b:"
  });
  private final byte [] startRow = Bytes.toBytes("0");

  private final TreeMap<byte [], SortedMap<byte [], Cell>> values =
    new TreeMap<byte [], SortedMap<byte [], Cell>>(Bytes.BYTES_COMPARATOR);
  
  /**
   * @throws Exception
   */
  public TestScannerAPI() throws Exception {
    super();
    try {
      TreeMap<byte [], Cell> columns =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      columns.put(Bytes.toBytes("a:1"),
          new Cell(Bytes.toBytes("1"), HConstants.LATEST_TIMESTAMP));
      values.put(Bytes.toBytes("1"), columns);
      columns = new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      columns.put(Bytes.toBytes("a:2"),
          new Cell(Bytes.toBytes("2"), HConstants.LATEST_TIMESTAMP));
      columns.put(Bytes.toBytes("b:2"),
          new Cell(Bytes.toBytes("2"), HConstants.LATEST_TIMESTAMP));
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
  
  /**
   * @throws IOException
   */
  public void testApi() throws IOException {
    final String tableName = getName();

    // Create table
    
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    for (int i = 0; i < columns.length; i++) {
      tableDesc.addFamily(new HColumnDescriptor(columns[i]));
    }
    admin.createTable(tableDesc);

    // Insert values
    
    HTable table = new HTable(conf, getName());

    for (Map.Entry<byte [], SortedMap<byte [], Cell>> row: values.entrySet()) {
      BatchUpdate b = new BatchUpdate(row.getKey());
      for (Map.Entry<byte [], Cell> val: row.getValue().entrySet()) {
        b.put(val.getKey(), val.getValue().getValue());
      }
      table.commit(b);
    }

    HRegion region = null;
    try {
      Collection<HRegion> regions =
        cluster.getRegionThreads().get(0).getRegionServer().getOnlineRegions();
      for (HRegion r: regions) {
        if (!r.getRegionInfo().isMetaRegion()) {
          region = r;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      IOException iox = new IOException("error finding region");
      iox.initCause(e);
      throw iox;
    }
    @SuppressWarnings("null")
    ScannerIncommon scanner = new InternalScannerIncommon(
      region.getScanner(columns, startRow, System.currentTimeMillis(), null));
    try {
      verify(scanner);
    } finally {
      scanner.close();
    }
    
    scanner = new ClientScannerIncommon(table.getScanner(columns, startRow));
    try {
      verify(scanner);
    } finally {
      scanner.close();
    }
    Scanner scanner2 = table.getScanner(columns, startRow);
    try {
      for (RowResult r : scanner2) {
        assertTrue("row key", values.containsKey(r.getRow()));

        SortedMap<byte [], Cell> columnValues = values.get(r.getRow());
        assertEquals(columnValues.size(), r.size());
        for (Map.Entry<byte [], Cell> e: columnValues.entrySet()) {
          byte [] column = e.getKey();
          assertTrue("column", r.containsKey(column));
          assertTrue("value", Arrays.equals(columnValues.get(column).getValue(),
            r.get(column).getValue()));
        }
      }      
    } finally {
      scanner.close();
    }
  }
  
  private void verify(ScannerIncommon scanner) throws IOException {
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (scanner.next(results)) {
      assertTrue("row key", values.containsKey(results.get(0).getRow()));
      // TODO FIX.
//      SortedMap<byte [], Cell> columnValues = values.get(row);
//      assertEquals(columnValues.size(), results.size());
//      for (Map.Entry<byte [], Cell> e: columnValues.entrySet()) {
//        byte [] column = e.getKey();
//        assertTrue("column", results.containsKey(column));
//        assertTrue("value", Arrays.equals(columnValues.get(column).getValue(),
//            results.get(column).getValue()));
//      }
//      
      results.clear();
    }
  }
}