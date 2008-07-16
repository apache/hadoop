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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;

/**
 * Test that verifies that scanners return a different timestamp for values that
 * are not stored at the same time. (HBASE-737)
 */
public class TestScannerTimes extends HBaseClusterTestCase {
  private static final String TABLE_NAME = "hbase737";
  private static final String FAM1 = "fam1:";
  private static final String FAM2 = "fam2:";
  private static final String ROW = "row";
  
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
    BatchUpdate b = new BatchUpdate(ROW);
    b.put(FAM1 + "letters", "abcdefg".getBytes(HConstants.UTF8_ENCODING));
    table.commit(b);
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }
    
    b = new BatchUpdate(ROW);
    b.put(FAM1 + "numbers", "123456".getBytes(HConstants.UTF8_ENCODING));
    table.commit(b);
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }
    
    b = new BatchUpdate(ROW);
    b.put(FAM2 + "letters", "hijklmnop".getBytes(HConstants.UTF8_ENCODING));
    table.commit(b);
    
    long times[] = new long[3];
    byte[][] columns = new byte[][] {
        FAM1.getBytes(HConstants.UTF8_ENCODING),
        FAM2.getBytes(HConstants.UTF8_ENCODING)
    };
    
    // First scan the memcache
    
    Scanner s = table.getScanner(columns);
    try {
      int index = 0;
      RowResult r = null;
      while ((r = s.next()) != null) {
        for (Cell c: r.values()) {
          times[index++] = c.getTimestamp();
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
    
    // Fush data to disk and try again
    
    cluster.flushcache();
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }
    
    s = table.getScanner(columns);
    try {
      int index = 0;
      RowResult r = null;
      while ((r = s.next()) != null) {
        for (Cell c: r.values()) {
          times[index++] = c.getTimestamp();
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
