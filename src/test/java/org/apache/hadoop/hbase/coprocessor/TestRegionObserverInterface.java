/**
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestRegionObserverInterface {
  static final Log LOG = LogFactory.getLog(TestRegionObserverInterface.class);
  static final String DIR = "test/build/data/TestRegionObserver/";

  public static final byte[] TEST_TABLE = Bytes.toBytes("TestTable");
  public final static byte[] A = Bytes.toBytes("a");
  public final static byte[] B = Bytes.toBytes("b");
  public final static byte[] C = Bytes.toBytes("c");
  public final static byte[] ROW = Bytes.toBytes("testrow");

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = util.getConfiguration();
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver");

    util.startMiniCluster();
    cluster = util.getMiniHBaseCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testRegionObserver() throws IOException {
    byte[] tableName = TEST_TABLE;
    // recreate table every time in order to reset the status of the
    // coproccessor.
    HTable table = util.createTable(tableName, new byte[][] {A, B, C});
    verifyMethodResult(SimpleRegionObserver.class,
        new String[] {"hadPreGet", "hadPostGet", "hadPrePut", "hadPostPut",
            "hadDelete"},
        TEST_TABLE,
        new Boolean[] {false, false, false, false, false});

    Put put = new Put(ROW);
    put.add(A, A, A);
    put.add(B, B, B);
    put.add(C, C, C);
    table.put(put);

    verifyMethodResult(SimpleRegionObserver.class,
        new String[] {"hadPreGet", "hadPostGet", "hadPrePut", "hadPostPut",
            "hadDelete"},
        TEST_TABLE,
        new Boolean[] {false, false, true, true, false}
    );

    Get get = new Get(ROW);
    get.addColumn(A, A);
    get.addColumn(B, B);
    get.addColumn(C, C);
    table.get(get);

    verifyMethodResult(SimpleRegionObserver.class,
        new String[] {"hadPreGet", "hadPostGet", "hadPrePut", "hadPostPut",
            "hadDelete"},
        TEST_TABLE,
        new Boolean[] {true, true, true, true, false}
    );

    Delete delete = new Delete(ROW);
    delete.deleteColumn(A, A);
    delete.deleteColumn(B, B);
    delete.deleteColumn(C, C);
    table.delete(delete);

    verifyMethodResult(SimpleRegionObserver.class,
        new String[] {"hadPreGet", "hadPostGet", "hadPrePut", "hadPostPut",
            "hadDelete"},
        TEST_TABLE,
        new Boolean[] {true, true, true, true, true}
    );
    util.deleteTable(tableName);
  }

  @Test
  public void testIncrementHook() throws IOException {
    byte[] tableName = TEST_TABLE;

    HTable table = util.createTable(tableName, new byte[][] {A, B, C});
    Increment inc = new Increment(Bytes.toBytes(0));
    inc.addColumn(A, A, 1);

    verifyMethodResult(SimpleRegionObserver.class,
        new String[] {"hadPreIncrement", "hadPostIncrement"},
        tableName,
        new Boolean[] {false, false}
    );

    table.increment(inc);

    verifyMethodResult(SimpleRegionObserver.class,
        new String[] {"hadPreIncrement", "hadPostIncrement"},
        tableName,
        new Boolean[] {true, true}
    );
    util.deleteTable(tableName);
  }

  @Test
  // HBase-3583
  public void testHBase3583() throws IOException {
    byte[] tableName = Bytes.toBytes("testHBase3583");
    util.createTable(tableName, new byte[][] {A, B, C});

    verifyMethodResult(SimpleRegionObserver.class,
        new String[] {"hadPreGet", "hadPostGet", "wasScannerNextCalled",
            "wasScannerCloseCalled"},
        tableName,
        new Boolean[] {false, false, false, false}
    );

    HTable table = new HTable(util.getConfiguration(), tableName);
    Put put = new Put(ROW);
    put.add(A, A, A);
    table.put(put);

    Get get = new Get(ROW);
    get.addColumn(A, A);
    table.get(get);

    // verify that scannerNext and scannerClose upcalls won't be invoked
    // when we perform get().
    verifyMethodResult(SimpleRegionObserver.class,
        new String[] {"hadPreGet", "hadPostGet", "wasScannerNextCalled",
            "wasScannerCloseCalled"},
        tableName,
        new Boolean[] {true, true, false, false}
    );

    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    try {
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      }
    } finally {
      scanner.close();
    }

    // now scanner hooks should be invoked.
    verifyMethodResult(SimpleRegionObserver.class,
        new String[] {"wasScannerNextCalled", "wasScannerCloseCalled"},
        tableName,
        new Boolean[] {true, true}
    );
    util.deleteTable(tableName);
  }

  @Test
  // HBase-3758
  public void testHBase3758() throws IOException {
    byte[] tableName = Bytes.toBytes("testHBase3758");
    util.createTable(tableName, new byte[][] {A, B, C});

    verifyMethodResult(SimpleRegionObserver.class,
        new String[] {"hadDeleted", "wasScannerOpenCalled"},
        tableName,
        new Boolean[] {false, false}
    );

    HTable table = new HTable(util.getConfiguration(), tableName);
    Put put = new Put(ROW);
    put.add(A, A, A);
    table.put(put);

    Delete delete = new Delete(ROW);
    table.delete(delete);

    verifyMethodResult(SimpleRegionObserver.class,
        new String[] {"hadDeleted", "wasScannerOpenCalled"},
        tableName,
        new Boolean[] {true, false}
    );

    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    try {
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      }
    } finally {
      scanner.close();
    }

    // now scanner hooks should be invoked.
    verifyMethodResult(SimpleRegionObserver.class,
        new String[] {"wasScannerOpenCalled"},
        tableName,
        new Boolean[] {true}
    );
    util.deleteTable(tableName);
  }

  /* Overrides compaction to only output rows with keys that are even numbers */
  public static class EvenOnlyCompactor extends BaseRegionObserver {
    long lastCompaction;
    long lastFlush;

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
        Store store, final InternalScanner scanner) {
      return new InternalScanner() {
        @Override
        public boolean next(List<KeyValue> results) throws IOException {
          return next(results, -1);
        }

        @Override
        public boolean next(List<KeyValue> results, int limit) throws IOException {
          List<KeyValue> internalResults = new ArrayList<KeyValue>();
          boolean hasMore;
          do {
            hasMore = scanner.next(internalResults, limit);
            if (!internalResults.isEmpty()) {
              long row = Bytes.toLong(internalResults.get(0).getRow());
              if (row % 2 == 0) {
                // return this row
                break;
              }
              // clear and continue
              internalResults.clear();
            }
          } while (hasMore);

          if (!internalResults.isEmpty()) {
            results.addAll(internalResults);
          }
          return hasMore;
        }

        @Override
        public void close() throws IOException {
          scanner.close();
        }
      };
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,
        Store store, StoreFile resultFile) {
      lastCompaction = EnvironmentEdgeManager.currentTimeMillis();
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e) {
      lastFlush = EnvironmentEdgeManager.currentTimeMillis();
    }
  }
  /**
   * Tests overriding compaction handling via coprocessor hooks
   * @throws Exception
   */
  @Test
  public void testCompactionOverride() throws Exception {
    byte[] compactTable = Bytes.toBytes("TestCompactionOverride");
    HBaseAdmin admin = util.getHBaseAdmin();
    if (admin.tableExists(compactTable)) {
      admin.disableTable(compactTable);
      admin.deleteTable(compactTable);
    }

    HTableDescriptor htd = new HTableDescriptor(compactTable);
    htd.addFamily(new HColumnDescriptor(A));
    htd.addCoprocessor(EvenOnlyCompactor.class.getName());
    admin.createTable(htd);

    HTable table = new HTable(util.getConfiguration(), compactTable);
    for (long i=1; i<=10; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put put = new Put(iBytes);
      put.setWriteToWAL(false);
      put.add(A, A, iBytes);
      table.put(put);
    }

    HRegion firstRegion = cluster.getRegions(compactTable).get(0);
    Coprocessor cp = firstRegion.getCoprocessorHost().findCoprocessor(
        EvenOnlyCompactor.class.getName());
    assertNotNull("EvenOnlyCompactor coprocessor should be loaded", cp);
    EvenOnlyCompactor compactor = (EvenOnlyCompactor)cp;

    // force a compaction
    long ts = System.currentTimeMillis();
    admin.flush(compactTable);
    // wait for flush
    for (int i=0; i<10; i++) {
      if (compactor.lastFlush >= ts) {
        break;
      }
      Thread.sleep(1000);
    }
    assertTrue("Flush didn't complete", compactor.lastFlush >= ts);
    LOG.debug("Flush complete");

    ts = compactor.lastFlush;
    admin.majorCompact(compactTable);
    // wait for compaction
    for (int i=0; i<30; i++) {
      if (compactor.lastCompaction >= ts) {
        break;
      }
      Thread.sleep(1000);
    }
    LOG.debug("Last compaction was at "+compactor.lastCompaction);
    assertTrue("Compaction didn't complete", compactor.lastCompaction >= ts);

    // only even rows should remain
    ResultScanner scanner = table.getScanner(new Scan());
    try {
      for (long i=2; i<=10; i+=2) {
        Result r = scanner.next();
        assertNotNull(r);
        assertFalse(r.isEmpty());
        byte[] iBytes = Bytes.toBytes(i);
        assertArrayEquals("Row should be "+i, r.getRow(), iBytes);
        assertArrayEquals("Value should be "+i, r.getValue(A, A), iBytes);
      }
    } finally {
      scanner.close();
    }
  }

  // check each region whether the coprocessor upcalls are called or not.
  private void verifyMethodResult(Class c, String methodName[], byte[] tableName,
                                  Object value[]) throws IOException {
    try {
      for (JVMClusterUtil.RegionServerThread t : cluster.getRegionServerThreads()) {
        for (HRegionInfo r : t.getRegionServer().getOnlineRegions()) {
          if (!Arrays.equals(r.getTableName(), tableName)) {
            continue;
          }
          RegionCoprocessorHost cph = t.getRegionServer().getOnlineRegion(r.getRegionName()).
              getCoprocessorHost();

          Coprocessor cp = cph.findCoprocessor(c.getName());
          assertNotNull(cp);
          for (int i = 0; i < methodName.length; ++i) {
            Method m = c.getMethod(methodName[i]);
            Object o = m.invoke(cp);
            assertTrue("Result of " + c.getName() + "." + methodName[i]
                + " is expected to be " + value[i].toString()
                + ", while we get " + o.toString(), o.equals(value[i]));
          }
        }
      }
    } catch (Exception e) {
      throw new IOException(e.toString());
    }
  }

  private static byte [][] makeN(byte [] base, int n) {
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(String.format("%02d", i)));
    }
    return ret;
  }
}

