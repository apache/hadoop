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
      10,  // Ten is arbitrary number.  Keep versions to help debuggging.
      Compression.Algorithm.NONE.getName(), false, true, 8 * 1024,
      HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),
      HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
    TESTTABLEDESC.addFamily(new HColumnDescriptor(B,
      10,  // Ten is arbitrary number.  Keep versions to help debuggging.
      Compression.Algorithm.NONE.getName(), false, true, 8 * 1024,
      HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),
      HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
    TESTTABLEDESC.addFamily(new HColumnDescriptor(C,
      10,  // Ten is arbitrary number.  Keep versions to help debuggging.
      Compression.Algorithm.NONE.getName(), false, true, 8 * 1024,
      HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),
      HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
  }

  /** HRegionInfo for root region */
  public static final HRegionInfo REGION_INFO =
    new HRegionInfo(TESTTABLEDESC, HConstants.EMPTY_BYTE_ARRAY,
    HConstants.EMPTY_BYTE_ARRAY);

  MiniDFSCluster cluster = null;
  HRegion r;

  @Override
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR,
      this.cluster.getFileSystem().getHomeDirectory().toString());
    super.setUp();
  }

  private int addWideContent(HRegion region) throws IOException {
    int count = 0;
    for (char c = 'a'; c <= 'c'; c++) {
      byte[] row = Bytes.toBytes("ab" + c);
      int i;
      for (i = 0; i < 2500; i++) {
        byte[] b = Bytes.toBytes(String.format("%10d", i));
        Put put = new Put(row);
        put.add(COLUMNS[rng.nextInt(COLUMNS.length)], b, b);
        region.put(put);
        count++;
      }
    }
    return count;
  }

  public void testWideScanBatching() throws IOException {
    try {
      this.r = createNewHRegion(REGION_INFO.getTableDesc(), null, null);
      int inserted = addWideContent(this.r);
      List<KeyValue> results = new ArrayList<KeyValue>();
      Scan scan = new Scan();
      scan.addFamily(A);
      scan.addFamily(B);
      scan.addFamily(C);
      scan.setBatch(1000);
      InternalScanner s = r.getScanner(scan);
      int total = 0;
      int i = 0;
      boolean more;
      do {
        more = s.next(results);
        i++;
        LOG.info("iteration #" + i + ", results.size=" + results.size());

        // assert that the result set is no larger than 1000
        assertTrue(results.size() <= 1000);

        total += results.size();

        if (results.size() > 0) {
          // assert that all results are from the same row
          byte[] row = results.get(0).getRow();
          for (KeyValue kv: results) {
            assertTrue(Bytes.equals(row, kv.getRow()));
          }
        }

        results.clear();
      } while (more);

      // assert that the scanner returned all values
      LOG.info("inserted " + inserted + ", scanned " + total);
      assertTrue(total == inserted);

      s.close();
    } finally {
      this.r.close();
      this.r.getLog().closeAndDelete();
      shutdownDfs(this.cluster);
    }
  }
}
