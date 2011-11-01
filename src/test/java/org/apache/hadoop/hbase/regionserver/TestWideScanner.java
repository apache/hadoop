/*
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestWideScanner extends HBaseTestCase {
  private final Log LOG = LogFactory.getLog(this.getClass());

  static final byte[] A = Bytes.toBytes("A");
  static final byte[] B = Bytes.toBytes("B");
  static final byte[] C = Bytes.toBytes("C");
  static byte[][] COLUMNS = { A, B, C };
  static final Random rng = new Random();
  static final HTableDescriptor TESTTABLEDESC =
    new HTableDescriptor("testwidescan");
  static {
    TESTTABLEDESC.addFamily(new HColumnDescriptor(A,
      100,  // Keep versions to help debuggging.
      Compression.Algorithm.NONE.getName(), false, true, 8 * 1024,
      HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),
      HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
    TESTTABLEDESC.addFamily(new HColumnDescriptor(B,
      100,  // Keep versions to help debuggging.
      Compression.Algorithm.NONE.getName(), false, true, 8 * 1024,
      HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),
      HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
    TESTTABLEDESC.addFamily(new HColumnDescriptor(C,
      100,  // Keep versions to help debuggging.
      Compression.Algorithm.NONE.getName(), false, true, 8 * 1024,
      HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),
      HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
  }

  /** HRegionInfo for root region */
  HRegion r;

  private int addWideContent(HRegion region) throws IOException {
    int count = 0;
    for (char c = 'a'; c <= 'c'; c++) {
      byte[] row = Bytes.toBytes("ab" + c);
      int i, j;
      long ts = System.currentTimeMillis();
      for (i = 0; i < 100; i++) {
        byte[] b = Bytes.toBytes(String.format("%10d", i));
        for (j = 0; j < 100; j++) {
          Put put = new Put(row);
          put.setWriteToWAL(false);
          put.add(COLUMNS[rng.nextInt(COLUMNS.length)], b, ++ts, b);
          region.put(put);
          count++;
        }
      }
    }
    return count;
  }

  public void testWideScanBatching() throws IOException {
    final int batch = 256;
    try {
      this.r = createNewHRegion(TESTTABLEDESC, null, null);
      int inserted = addWideContent(this.r);
      List<KeyValue> results = new ArrayList<KeyValue>();
      Scan scan = new Scan();
      scan.addFamily(A);
      scan.addFamily(B);
      scan.addFamily(C);
      scan.setMaxVersions(100);
      scan.setBatch(batch);
      InternalScanner s = r.getScanner(scan);
      int total = 0;
      int i = 0;
      boolean more;
      do {
        more = s.next(results);
        i++;
        LOG.info("iteration #" + i + ", results.size=" + results.size());

        // assert that the result set is no larger
        assertTrue(results.size() <= batch);

        total += results.size();

        if (results.size() > 0) {
          // assert that all results are from the same row
          byte[] row = results.get(0).getRow();
          for (KeyValue kv: results) {
            assertTrue(Bytes.equals(row, kv.getRow()));
          }
        }

        results.clear();

        // trigger ChangedReadersObservers
        Iterator<KeyValueScanner> scanners =
          ((HRegion.RegionScannerImpl)s).storeHeap.getHeap().iterator();
        while (scanners.hasNext()) {
          StoreScanner ss = (StoreScanner)scanners.next();
          ss.updateReaders();
        }
      } while (more);

      // assert that the scanner returned all values
      LOG.info("inserted " + inserted + ", scanned " + total);
      assertEquals(total, inserted);

      s.close();
    } finally {
      this.r.close();
      this.r.getLog().closeAndDelete();
    }
  }
}
