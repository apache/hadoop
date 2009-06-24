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

package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test that verifies that scanners return a different timestamp for values that
 * are not stored at the same time. (HBASE-737)
 */
public class TestScannerTimes extends HBaseClusterTestCase {
  private static final String TABLE_NAME = "hbase737";
  private static final byte [] FAM1 = Bytes.toBytes("fam1");
  private static final byte [] FAM2 = Bytes.toBytes("fam2");
  private static final byte [] ROW = Bytes.toBytes("row");
  
  /**
   * test for HBASE-737
   * @throws IOException
   */
  public void testHBase737 () throws IOException {
    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(FAM1));
    desc.addFamily(new HColumnDescriptor(FAM2));
    
    // Create table
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    
    // Open table
    HTable table = new HTable(conf, TABLE_NAME);
    
    // Insert some values
    Put put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("letters"), Bytes.toBytes("abcdefg"));
    table.put(put);
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }
    
    put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("numbers"), Bytes.toBytes("123456"));
    table.put(put);
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM2, Bytes.toBytes("letters"), Bytes.toBytes("hijklmnop"));
    table.put(put);
    
    long times[] = new long[3];
    
    // First scan the memstore
    
    Scan scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    ResultScanner s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(KeyValue key : r.sorted()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }
    
    // Flush data to disk and try again
    
    cluster.flushcache();
    
    // Reset times
    for(int i=0;i<times.length;i++) {
      times[i] = 0;
    }
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }
    scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(KeyValue key : r.sorted()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }
  }
}
