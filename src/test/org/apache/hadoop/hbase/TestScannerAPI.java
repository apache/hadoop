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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;

/** test the scanner API at all levels */
public class TestScannerAPI extends HBaseClusterTestCase {
  private final Text[] columns = new Text[] {
    new Text("a:"),
    new Text("b:")
  };
  private final Text startRow = new Text("0");

  private final TreeMap<Text, SortedMap<Text, byte[]>> values =
    new TreeMap<Text, SortedMap<Text, byte[]>>();
  
  /**
   * @throws Exception
   */
  public TestScannerAPI() throws Exception {
    super();
    try {
      TreeMap<Text, byte[]> columns = new TreeMap<Text, byte[]>();
      columns.put(new Text("a:1"), "1".getBytes(HConstants.UTF8_ENCODING));
      values.put(new Text("1"), columns);
      columns = new TreeMap<Text, byte[]>();
      columns.put(new Text("a:2"), "2".getBytes(HConstants.UTF8_ENCODING));
      columns.put(new Text("b:2"), "2".getBytes(HConstants.UTF8_ENCODING));
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
      tableDesc.addFamily(new HColumnDescriptor(columns[i].toString()));
    }
    admin.createTable(tableDesc);

    // Insert values
    
    HTable table = new HTable(conf, new Text(getName()));

    for (Map.Entry<Text, SortedMap<Text, byte[]>> row: values.entrySet()) {
      long lockid = table.startUpdate(row.getKey());
      for (Map.Entry<Text, byte[]> val: row.getValue().entrySet()) {
        table.put(lockid, val.getKey(), val.getValue());
      }
      table.commit(lockid);
    }

    HRegion region = null;
    try {
      SortedMap<Text, HRegion> regions =
        cluster.getRegionThreads().get(0).getRegionServer().getOnlineRegions();
      for (Map.Entry<Text, HRegion> e: regions.entrySet()) {
        if (!e.getValue().getRegionInfo().isMetaRegion()) {
          region = e.getValue();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      IOException iox = new IOException("error finding region");
      iox.initCause(e);
      throw iox;
    }
    @SuppressWarnings("null")
    HScannerInterface scanner = 
      region.getScanner(columns, startRow, System.currentTimeMillis(), null);
    try {
      verify(scanner);
    } finally {
      scanner.close();
    }
    
    scanner = table.obtainScanner(columns, startRow);
    try {
      verify(scanner);
    } finally {
      scanner.close();
    }
    scanner = table.obtainScanner(columns, startRow);
    try {
      for (Iterator<Map.Entry<HStoreKey, SortedMap<Text, byte[]>>> iterator =
        scanner.iterator();
      iterator.hasNext();
      ) {
        Map.Entry<HStoreKey, SortedMap<Text, byte[]>> row = iterator.next();
        HStoreKey key = row.getKey();
        assertTrue("row key", values.containsKey(key.getRow()));

        SortedMap<Text, byte[]> results = row.getValue();
        SortedMap<Text, byte[]> columnValues = values.get(key.getRow());
        assertEquals(columnValues.size(), results.size());
        for (Map.Entry<Text, byte[]> e: columnValues.entrySet()) {
          Text column = e.getKey();
          assertTrue("column", results.containsKey(column));
          assertTrue("value", Arrays.equals(columnValues.get(column),
              results.get(column)));
        }
      }
    } finally {
      scanner.close();
    }
  }
  
  private void verify(HScannerInterface scanner) throws IOException {
    HStoreKey key = new HStoreKey();
    SortedMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
    while (scanner.next(key, results)) {
      Text row = key.getRow();
      assertTrue("row key", values.containsKey(row));
      
      SortedMap<Text, byte[]> columnValues = values.get(row);
      assertEquals(columnValues.size(), results.size());
      for (Map.Entry<Text, byte[]> e: columnValues.entrySet()) {
        Text column = e.getKey();
        assertTrue("column", results.containsKey(column));
        assertTrue("value", Arrays.equals(columnValues.get(column),
            results.get(column)));
      }
      results.clear();
    }
  }
}
