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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
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
    this.desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));

    // Region 0 will contain the key range [,row_0500)
    INFOS[0] = new HRegionInfo(desc.getName(), HConstants.EMPTY_START_ROW,
        Bytes.toBytes("row_0500"));
    // Region 1 will contain the key range [row_0500,)
    INFOS[1] = new HRegionInfo(desc.getName(), Bytes.toBytes("row_0500"),
        HConstants.EMPTY_END_ROW);

    // Create root and meta regions
    createRootAndMetaRegions();
    // Create the regions
    for (int i = 0; i < REGIONS.length; i++) {
      REGIONS[i] =
        HRegion.createHRegion(this.INFOS[i], this.testDir, this.conf,
            this.desc);
      // Insert data
      for (int j = 0; j < TIMESTAMPS.length; j++) {
        Put put = new Put(ROWS[i], TIMESTAMPS[j], null);
        put.add(HConstants.CATALOG_FAMILY, null, TIMESTAMPS[j],
            Bytes.toBytes(TIMESTAMPS[j]));
        REGIONS[i].put(put);
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
        Get get = new Get(ROWS[i]);
        get.addFamily(HConstants.CATALOG_FAMILY);
        get.setTimeStamp(TIMESTAMPS[j]);
        Result result = t.get(get);
        int cellCount = 0;
        for(@SuppressWarnings("unused")KeyValue kv : result.sorted()) {
          cellCount++;
        }
        assertTrue(cellCount == 1);
      }
    }

    // Case 1: scan with LATEST_TIMESTAMP. Should get two rows
    int count = 0;
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner s = t.getScanner(scan);
    try {
      for (Result rr = null; (rr = s.next()) != null;) {
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
    scan = new Scan();
    scan.setTimeRange(1000L, Long.MAX_VALUE);
    scan.addFamily(HConstants.CATALOG_FAMILY);

    s = t.getScanner(scan);
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
    scan = new Scan();
    scan.setTimeStamp(1000L);
    scan.addFamily(HConstants.CATALOG_FAMILY);

    s = t.getScanner(scan);
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
    scan = new Scan();
    scan.setTimeRange(100L, 1000L);
    scan.addFamily(HConstants.CATALOG_FAMILY);

    s = t.getScanner(scan);
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
    scan = new Scan();
    scan.setTimeStamp(100L);
    scan.addFamily(HConstants.CATALOG_FAMILY);

    s = t.getScanner(scan);
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
