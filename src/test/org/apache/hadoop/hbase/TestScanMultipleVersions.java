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

package org.apache.hadoop.hbase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Regression test for HBASE-613
 */
public class TestScanMultipleVersions extends HBaseClusterTestCase {
  private final byte[] TABLE_NAME = Bytes.toBytes("TestScanMultipleVersions");
  private final HRegionInfo[] INFOS = new HRegionInfo[2];
  private final HRegion[] REGIONS = new HRegion[2];
  private final byte[][] ROWS = new byte[][] {
      Bytes.toBytes("row_0200"),
      Bytes.toBytes("row_0800")
  };
  private final long[] TIMESTAMPS = new long[] {
      100L,
      1000L
  };
  private HTableDescriptor desc = null;

  @Override
  protected void preHBaseClusterSetup() throws Exception {
    testDir = new Path(conf.get(HConstants.HBASE_DIR));
    
    // Create table description
    
    this.desc = new HTableDescriptor(TABLE_NAME);
    this.desc.addFamily(new HColumnDescriptor(HConstants.COLUMN_FAMILY));

    // Region 0 will contain the key range [,row_0500)
    INFOS[0] = new HRegionInfo(this.desc, HConstants.EMPTY_START_ROW,
        Bytes.toBytes("row_0500"));
    // Region 1 will contain the key range [row_0500,)
    INFOS[1] = new HRegionInfo(this.desc, Bytes.toBytes("row_0500"),
        HConstants.EMPTY_END_ROW);

    // Create root and meta regions
    createRootAndMetaRegions();
    // Create the regions
    for (int i = 0; i < REGIONS.length; i++) {
      REGIONS[i] =
        HRegion.createHRegion(this.INFOS[i], this.testDir, this.conf);
      // Insert data
      for (int j = 0; j < TIMESTAMPS.length; j++) {
        BatchUpdate b = new BatchUpdate(ROWS[i], TIMESTAMPS[j]);
        b.put(HConstants.COLUMN_FAMILY, Bytes.toBytes(TIMESTAMPS[j]));
        REGIONS[i].batchUpdate(b, null);
      }
      // Insert the region we created into the meta
      HRegion.addRegionToMETA(meta, REGIONS[i]);
      // Close region
      REGIONS[i].close();
      REGIONS[i].getLog().closeAndDelete();
    }
    // Close root and meta regions
    closeRootAndMeta();
  }
  
  /**
   * @throws Exception
   */
  public void testScanMultipleVersions() throws Exception {
    // At this point we have created multiple regions and both HDFS and HBase
    // are running. There are 5 cases we have to test. Each is described below.
    HTable t = new HTable(conf, TABLE_NAME);
    for (int i = 0; i < ROWS.length; i++) {
      for (int j = 0; j < TIMESTAMPS.length; j++) {
        Cell [] cells =
          t.get(ROWS[i], HConstants.COLUMN_FAMILY, TIMESTAMPS[j], 1);
        assertTrue(cells != null && cells.length == 1);
        System.out.println("Row=" + Bytes.toString(ROWS[i]) + ", cell=" +
          cells[0]);
      }
    }
    
    // Case 1: scan with LATEST_TIMESTAMP. Should get two rows
    int count = 0;
    Scanner s = t.getScanner(HConstants.COLUMN_FAMILY_ARRAY);
    try {
      for (RowResult rr = null; (rr = s.next()) != null;) {
        System.out.println(rr.toString());
        count += 1;
      }
      assertEquals("Number of rows should be 2", 2, count);
    } finally {
      s.close();
    }

    // Case 2: Scan with a timestamp greater than most recent timestamp
    // (in this case > 1000 and < LATEST_TIMESTAMP. Should get 2 rows.
    
    count = 0;
    s = t.getScanner(HConstants.COLUMN_FAMILY_ARRAY, HConstants.EMPTY_START_ROW,
        10000L);
    try {
      while (s.next() != null) {
        count += 1;
      }
      assertEquals("Number of rows should be 2", 2, count);
    } finally {
      s.close();
    }
    
    // Case 3: scan with timestamp equal to most recent timestamp
    // (in this case == 1000. Should get 2 rows.
    
    count = 0;
    s = t.getScanner(HConstants.COLUMN_FAMILY_ARRAY, HConstants.EMPTY_START_ROW,
        1000L);
    try {
      while (s.next() != null) {
        count += 1;
      }
      assertEquals("Number of rows should be 2", 2, count);
    } finally {
      s.close();
    }
    
    // Case 4: scan with timestamp greater than first timestamp but less than
    // second timestamp (100 < timestamp < 1000). Should get 2 rows.
    
    count = 0;
    s = t.getScanner(HConstants.COLUMN_FAMILY_ARRAY, HConstants.EMPTY_START_ROW,
        500L);
    try {
      while (s.next() != null) {
        count += 1;
      }
      assertEquals("Number of rows should be 2", 2, count);
    } finally {
      s.close();
    }
    
    // Case 5: scan with timestamp equal to first timestamp (100)
    // Should get 2 rows.
    
    count = 0;
    s = t.getScanner(HConstants.COLUMN_FAMILY_ARRAY, HConstants.EMPTY_START_ROW,
        100L);
    try {
      while (s.next() != null) {
        count += 1;
      }
      assertEquals("Number of rows should be 2", 2, count);
    } finally {
      s.close();
    }
  }
}
