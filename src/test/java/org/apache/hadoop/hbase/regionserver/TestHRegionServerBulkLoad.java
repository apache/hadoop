/**
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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests bulk loading of HFiles and shows the atomicity or lack of atomicity of
 * the region server's bullkLoad functionality.
 */
public class TestHRegionServerBulkLoad {
  final static Log LOG = LogFactory.getLog(TestHRegionServerBulkLoad.class);
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private final static Configuration conf = UTIL.getConfiguration();
  private final static byte[] QUAL = Bytes.toBytes("qual");
  private final static int NUM_CFS = 10;
  public static int BLOCKSIZE = 64 * 1024;
  public static String COMPRESSION = Compression.Algorithm.NONE.getName();

  private final static byte[][] families = new byte[NUM_CFS][];
  static {
    for (int i = 0; i < NUM_CFS; i++) {
      families[i] = Bytes.toBytes(family(i));
    }
  }

  static byte[] rowkey(int i) {
    return Bytes.toBytes(String.format("row_%08d", i));
  }

  static String family(int i) {
    return String.format("family_%04d", i);
  }

  /**
   * Create an HFile with the given number of rows with a specified value.
   */
  public static void createHFile(FileSystem fs, Path path, byte[] family,
      byte[] qualifier, byte[] value, int numRows) throws IOException {
    HFile.Writer writer = HFile
        .getWriterFactory(conf, new CacheConfig(conf))
        .createWriter(fs, path, BLOCKSIZE, COMPRESSION, KeyValue.KEY_COMPARATOR);
    long now = System.currentTimeMillis();
    try {
      // subtract 2 since iterateOnSplits doesn't include boundary keys
      for (int i = 0; i < numRows; i++) {
        KeyValue kv = new KeyValue(rowkey(i), family, qualifier, now, value);
        writer.append(kv);
      }
    } finally {
      writer.close();
    }
  }

  /**
   * Thread that does full scans of the table looking for any partially
   * completed rows.
   * 
   * Each iteration of this loads 10 hdfs files, which occupies 5 file open file
   * handles. So every 10 iterations (500 file handles) it does a region
   * compaction to reduce the number of open file handles.
   */
  public static class AtomicHFileLoader extends RepeatingTestThread {
    final AtomicLong numBulkLoads = new AtomicLong();
    final AtomicLong numCompactions = new AtomicLong();
    private String tableName;

    public AtomicHFileLoader(String tableName, TestContext ctx,
        byte targetFamilies[][]) throws IOException {
      super(ctx);
      this.tableName = tableName;
    }

    public void doAnAction() throws Exception {
      long iteration = numBulkLoads.getAndIncrement();
      Path dir =  UTIL.getDataTestDir(String.format("bulkLoad_%08d",
          iteration));

      // create HFiles for different column families
      FileSystem fs = UTIL.getTestFileSystem();
      byte[] val = Bytes.toBytes(String.format("%010d", iteration));
      final List<Pair<byte[], String>> famPaths = new ArrayList<Pair<byte[], String>>(
          NUM_CFS);
      for (int i = 0; i < NUM_CFS; i++) {
        Path hfile = new Path(dir, family(i));
        byte[] fam = Bytes.toBytes(family(i));
        createHFile(fs, hfile, fam, QUAL, val, 1000);
        famPaths.add(new Pair<byte[], String>(fam, hfile.toString()));
      }

      // bulk load HFiles
      HConnection conn = UTIL.getHBaseAdmin().getConnection();
      byte[] tbl = Bytes.toBytes(tableName);
      conn.getRegionServerWithRetries(new ServerCallable<Void>(conn, tbl, Bytes
          .toBytes("aaa")) {
        @Override
        public Void call() throws Exception {
          LOG.debug("Going to connect to server " + location + " for row "
              + Bytes.toStringBinary(row));
          byte[] regionName = location.getRegionInfo().getRegionName();
          server.bulkLoadHFiles(famPaths, regionName);
          return null;
        }
      });

      // Periodically do compaction to reduce the number of open file handles.
      if (numBulkLoads.get() % 10 == 0) {
        // 10 * 50 = 500 open file handles!
        conn.getRegionServerWithRetries(new ServerCallable<Void>(conn, tbl,
            Bytes.toBytes("aaa")) {
          @Override
          public Void call() throws Exception {
            LOG.debug("compacting " + location + " for row "
                + Bytes.toStringBinary(row));
            server.compactRegion(location.getRegionInfo(), true);
            numCompactions.incrementAndGet();
            return null;
          }
        });
      }
    }
  }

  /**
   * Thread that does full scans of the table looking for any partially
   * completed rows.
   */
  public static class AtomicScanReader extends RepeatingTestThread {
    byte targetFamilies[][];
    HTable table;
    AtomicLong numScans = new AtomicLong();
    AtomicLong numRowsScanned = new AtomicLong();
    String TABLE_NAME;

    public AtomicScanReader(String TABLE_NAME, TestContext ctx,
        byte targetFamilies[][]) throws IOException {
      super(ctx);
      this.TABLE_NAME = TABLE_NAME;
      this.targetFamilies = targetFamilies;
      table = new HTable(conf, TABLE_NAME);
    }

    public void doAnAction() throws Exception {
      Scan s = new Scan();
      for (byte[] family : targetFamilies) {
        s.addFamily(family);
      }
      ResultScanner scanner = table.getScanner(s);

      for (Result res : scanner) {
        byte[] lastRow = null, lastFam = null, lastQual = null;
        byte[] gotValue = null;
        for (byte[] family : targetFamilies) {
          byte qualifier[] = QUAL;
          byte thisValue[] = res.getValue(family, qualifier);
          if (gotValue != null && thisValue != null
              && !Bytes.equals(gotValue, thisValue)) {

            StringBuilder msg = new StringBuilder();
            msg.append("Failed on scan ").append(numScans)
                .append(" after scanning ").append(numRowsScanned)
                .append(" rows!\n");
            msg.append("Current  was " + Bytes.toString(res.getRow()) + "/"
                + Bytes.toString(family) + ":" + Bytes.toString(qualifier)
                + " = " + Bytes.toString(thisValue) + "\n");
            msg.append("Previous  was " + Bytes.toString(lastRow) + "/"
                + Bytes.toString(lastFam) + ":" + Bytes.toString(lastQual)
                + " = " + Bytes.toString(gotValue));
            throw new RuntimeException(msg.toString());
          }

          lastFam = family;
          lastQual = qualifier;
          lastRow = res.getRow();
          gotValue = thisValue;
        }
        numRowsScanned.getAndIncrement();
      }
      numScans.getAndIncrement();
    }
  }

  /**
   * Creates a table with given table name and specified number of column
   * families if the table does not already exist.
   */
  private void setupTable(String table, int cfs) throws IOException {
    try {
      LOG.info("Creating table " + table);
      HTableDescriptor htd = new HTableDescriptor(table);
      for (int i = 0; i < 10; i++) {
        htd.addFamily(new HColumnDescriptor(family(i)));
      }

      HBaseAdmin admin = UTIL.getHBaseAdmin();
      admin.createTable(htd);
    } catch (TableExistsException tee) {
      LOG.info("Table " + table + " already exists");
    }
  }

  /**
   * Atomic bulk load.
   */
  @Test
  public void testAtomicBulkLoad() throws Exception {
    String TABLE_NAME = "atomicBulkLoad";

    int millisToRun = 30000;
    int numScanners = 50;

    UTIL.startMiniCluster(1);
    try {
      runAtomicBulkloadTest(TABLE_NAME, millisToRun, numScanners);
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  void runAtomicBulkloadTest(String tableName, int millisToRun, int numScanners)
      throws Exception {
    setupTable(tableName, 10);

    TestContext ctx = new TestContext(UTIL.getConfiguration());

    AtomicHFileLoader loader = new AtomicHFileLoader(tableName, ctx, null);
    ctx.addThread(loader);

    List<AtomicScanReader> scanners = Lists.newArrayList();
    for (int i = 0; i < numScanners; i++) {
      AtomicScanReader scanner = new AtomicScanReader(tableName, ctx, families);
      scanners.add(scanner);
      ctx.addThread(scanner);
    }

    ctx.startThreads();
    ctx.waitFor(millisToRun);
    ctx.stop();

    LOG.info("Loaders:");
    LOG.info("  loaded " + loader.numBulkLoads.get());
    LOG.info("  compations " + loader.numCompactions.get());

    LOG.info("Scanners:");
    for (AtomicScanReader scanner : scanners) {
      LOG.info("  scanned " + scanner.numScans.get());
      LOG.info("  verified " + scanner.numRowsScanned.get() + " rows");
    }
  }

  /**
   * Run test on an HBase instance for 5 minutes. This assumes that the table
   * under test only has a single region.
   */
  public static void main(String args[]) throws Exception {
    try {
      Configuration c = HBaseConfiguration.create();
      TestHRegionServerBulkLoad test = new TestHRegionServerBulkLoad();
      test.setConf(c);
      test.runAtomicBulkloadTest("atomicTableTest", 5 * 60 * 1000, 50);
    } finally {
      System.exit(0); // something hangs (believe it is lru threadpool)
    }
  }

  private void setConf(Configuration c) {
    UTIL = new HBaseTestingUtility(c);
  }
}
