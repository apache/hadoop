/**
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
package org.apache.hadoop.hbase.regionserver.transactional;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.transactional.CommitUnsuccessfulException;
import org.apache.hadoop.hbase.client.transactional.HBaseBackedTransactionLogger;
import org.apache.hadoop.hbase.client.transactional.TransactionManager;
import org.apache.hadoop.hbase.client.transactional.TransactionState;
import org.apache.hadoop.hbase.client.transactional.TransactionalTable;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

public class TestTHLogRecovery extends HBaseClusterTestCase {
  private static final Log LOG = LogFactory.getLog(TestTHLogRecovery.class);

  private static final String TABLE_NAME = "table1";

  private static final byte[] FAMILY_COLON = Bytes.toBytes("family:");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] QUAL_A = Bytes.toBytes("a");
  private static final byte[] COL_A = Bytes.toBytes("family:a");

  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] ROW3 = Bytes.toBytes("row3");
  private static final int TOTAL_VALUE = 10;

  private HBaseAdmin admin;
  private TransactionManager transactionManager;
  private TransactionalTable table;

  /** constructor */
  public TestTHLogRecovery() {
    super(2, false);

    conf.set(HConstants.REGION_SERVER_CLASS, TransactionalRegionInterface.class
        .getName());
    conf.set(HConstants.REGION_SERVER_IMPL, TransactionalRegionServer.class
        .getName());

    // Set flush params so we don't get any
    // FIXME (defaults are probably fine)

    // Copied from TestRegionServerExit
    conf.setInt("ipc.client.connect.max.retries", 5); // reduce ipc retries
    conf.setInt("ipc.client.timeout", 10000); // and ipc timeout
    conf.setInt("hbase.client.pause", 10000); // increase client timeout
    conf.setInt("hbase.client.retries.number", 10); // increase HBase retries
  }

  @Override
  protected void setUp() throws Exception {
    FileSystem.getLocal(conf).delete(new Path(conf.get(HConstants.HBASE_DIR)),
        true);
    super.setUp();

    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    table = new TransactionalTable(conf, desc.getName());
    HBaseBackedTransactionLogger.createTable();

    transactionManager = new TransactionManager(
        new HBaseBackedTransactionLogger(), conf);
    writeInitalRows();
  }

  private void writeInitalRows() throws IOException {

    table.put(new Put(ROW1).add(FAMILY, QUAL_A, Bytes.toBytes(TOTAL_VALUE)));
    table.put(new Put(ROW2).add(FAMILY, QUAL_A, Bytes.toBytes(0)));
    table.put(new Put(ROW3).add(FAMILY, QUAL_A, Bytes.toBytes(0)));
  }

  public void testWithoutFlush() throws IOException,
      CommitUnsuccessfulException {
    writeInitalRows();
    TransactionState state1 = makeTransaction(false);
    transactionManager.tryCommit(state1);
    stopOrAbortRegionServer(true);

    Thread t = startVerificationThread(1);
    t.start();
    threadDumpingJoin(t);
  }

  public void testWithFlushBeforeCommit() throws IOException,
      CommitUnsuccessfulException {
    writeInitalRows();
    TransactionState state1 = makeTransaction(false);
    flushRegionServer();
    transactionManager.tryCommit(state1);
    stopOrAbortRegionServer(true);

    Thread t = startVerificationThread(1);
    t.start();
    threadDumpingJoin(t);
  }

  // FIXME, TODO
  // public void testWithFlushBetweenTransactionWrites() {
  // fail();
  // }

  private void flushRegionServer() {
    List<LocalHBaseCluster.RegionServerThread> regionThreads = cluster
        .getRegionThreads();

    HRegion region = null;
    int server = -1;
    for (int i = 0; i < regionThreads.size() && server == -1; i++) {
      HRegionServer s = regionThreads.get(i).getRegionServer();
      Collection<HRegion> regions = s.getOnlineRegions();
      for (HRegion r : regions) {
        if (Bytes.equals(r.getTableDesc().getName(), Bytes.toBytes(TABLE_NAME))) {
          server = i;
          region = r;
        }
      }
    }
    if (server == -1) {
      LOG.fatal("could not find region server serving table region");
      fail();
    }
    ((TransactionalRegionServer) regionThreads.get(server).getRegionServer())
        .getFlushRequester().request(region);
  }

  /**
   * Stop the region server serving TABLE_NAME.
   * 
   * @param abort set to true if region server should be aborted, if false it is
   * just shut down.
   */
  private void stopOrAbortRegionServer(final boolean abort) {
    List<LocalHBaseCluster.RegionServerThread> regionThreads = cluster
        .getRegionThreads();

    int server = -1;
    for (int i = 0; i < regionThreads.size(); i++) {
      HRegionServer s = regionThreads.get(i).getRegionServer();
      Collection<HRegion> regions = s.getOnlineRegions();
      LOG.info("server: " + regionThreads.get(i).getName());
      for (HRegion r : regions) {
        LOG.info("region: " + r.getRegionInfo().getRegionNameAsString());
        if (Bytes.equals(r.getTableDesc().getName(), Bytes.toBytes(TABLE_NAME))) {
          server = i;
        }
      }
    }
    if (server == -1) {
      LOG.fatal("could not find region server serving table region");
      fail();
    }
    if (abort) {
      this.cluster.abortRegionServer(server);

    } else {
      this.cluster.stopRegionServer(server, false);
    }
    LOG.info(this.cluster.waitOnRegionServer(server) + " has been "
        + (abort ? "aborted" : "shut down"));
  }

  private void verify(final int numRuns) throws IOException {
    // Reads
    int row1 = Bytes.toInt(table.get(new Get(ROW1).addColumn(FAMILY, QUAL_A))
        .getValue(FAMILY, QUAL_A));
    int row2 = Bytes.toInt(table.get(new Get(ROW2).addColumn(FAMILY, QUAL_A))
        .getValue(FAMILY, QUAL_A));
    int row3 = Bytes.toInt(table.get(new Get(ROW3).addColumn(FAMILY, QUAL_A))
        .getValue(FAMILY, QUAL_A));

    assertEquals(TOTAL_VALUE - 2 * numRuns, row1);
    assertEquals(numRuns, row2);
    assertEquals(numRuns, row3);
  }

  // Move 2 out of ROW1 and 1 into ROW2 and 1 into ROW3
  private TransactionState makeTransaction(final boolean flushMidWay)
      throws IOException {
    TransactionState transactionState = transactionManager.beginTransaction();

    // Reads
    int row1 = Bytes.toInt(table.get(transactionState,
        new Get(ROW1).addColumn(FAMILY, QUAL_A)).getValue(FAMILY, QUAL_A));
    int row2 = Bytes.toInt(table.get(transactionState,
        new Get(ROW2).addColumn(FAMILY, QUAL_A)).getValue(FAMILY, QUAL_A));
    int row3 = Bytes.toInt(table.get(transactionState,
        new Get(ROW3).addColumn(FAMILY, QUAL_A)).getValue(FAMILY, QUAL_A));

    row1 -= 2;
    row2 += 1;
    row3 += 1;

    if (flushMidWay) {
      flushRegionServer();
    }

    // Writes
    Put write = new Put(ROW1);
    write.add(FAMILY, QUAL_A, Bytes.toBytes(row1));
    table.put(transactionState, write);

    write = new Put(ROW2);
    write.add(FAMILY, QUAL_A, Bytes.toBytes(row2));
    table.put(transactionState, write);

    write = new Put(ROW3);
    write.add(FAMILY, QUAL_A, Bytes.toBytes(row3));
    table.put(transactionState, write);

    return transactionState;
  }

  /*
   * Run verification in a thread so I can concurrently run a thread-dumper
   * while we're waiting (because in this test sometimes the meta scanner looks
   * to be be stuck). @param tableName Name of table to find. @param row Row we
   * expect to find. @return Verification thread. Caller needs to calls start on
   * it.
   */
  private Thread startVerificationThread(final int numRuns) {
    Runnable runnable = new Runnable() {
      public void run() {
        try {
          // Now try to open a scanner on the meta table. Should stall until
          // meta server comes back up.
          HTable t = new HTable(conf, TABLE_NAME);
          Scan s = new Scan();
          s.addColumn(FAMILY, QUAL_A);
          ResultScanner scanner = t.getScanner(s);
          scanner.close();

        } catch (IOException e) {
          LOG.fatal("could not re-open meta table because", e);
          fail();
        }

        try {
          verify(numRuns);
          LOG.info("Success!");
        } catch (Exception e) {
          e.printStackTrace();
          fail();
        }
      }
    };
    return new Thread(runnable);
  }
}
