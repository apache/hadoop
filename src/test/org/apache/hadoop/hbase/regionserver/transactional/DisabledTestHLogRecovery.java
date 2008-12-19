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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.client.transactional.CommitUnsuccessfulException;
import org.apache.hadoop.hbase.client.transactional.TransactionManager;
import org.apache.hadoop.hbase.client.transactional.TransactionState;
import org.apache.hadoop.hbase.client.transactional.TransactionalTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

public class DisabledTestHLogRecovery extends HBaseClusterTestCase {
  private static final Log LOG = LogFactory.getLog(DisabledTestHLogRecovery.class);

  private static final String TABLE_NAME = "table1";

  private static final byte[] FAMILY = Bytes.toBytes("family:");
  private static final byte[] COL_A = Bytes.toBytes("family:a");

  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] ROW3 = Bytes.toBytes("row3");
  private static final int TOTAL_VALUE = 10;

  private HBaseAdmin admin;
  private TransactionManager transactionManager;
  private TransactionalTable table;

  /** constructor */
  public DisabledTestHLogRecovery() {
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
    FileSystem.getLocal(conf).delete(new Path(conf.get(HConstants.HBASE_DIR)), true);
    super.setUp();

    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    table = new TransactionalTable(conf, desc.getName());

    transactionManager = new TransactionManager(conf);
    writeInitalRows();
  }

  private void writeInitalRows() throws IOException {
    BatchUpdate update = new BatchUpdate(ROW1);
    update.put(COL_A, Bytes.toBytes(TOTAL_VALUE));
    table.commit(update);
    update = new BatchUpdate(ROW2);
    update.put(COL_A, Bytes.toBytes(0));
    table.commit(update);
    update = new BatchUpdate(ROW3);
    update.put(COL_A, Bytes.toBytes(0));
    table.commit(update);
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
    int row1 = Bytes.toInt(table.get(ROW1, COL_A).getValue());
    int row2 = Bytes.toInt(table.get(ROW2, COL_A).getValue());
    int row3 = Bytes.toInt(table.get(ROW3, COL_A).getValue());

    assertEquals(TOTAL_VALUE - 2 * numRuns, row1);
    assertEquals(numRuns, row2);
    assertEquals(numRuns, row3);
  }

  // Move 2 out of ROW1 and 1 into ROW2 and 1 into ROW3
  private TransactionState makeTransaction(final boolean flushMidWay)
      throws IOException {
    TransactionState transactionState = transactionManager.beginTransaction();

    // Reads
    int row1 = Bytes.toInt(table.get(transactionState, ROW1, COL_A).getValue());
    int row2 = Bytes.toInt(table.get(transactionState, ROW2, COL_A).getValue());
    int row3 = Bytes.toInt(table.get(transactionState, ROW3, COL_A).getValue());

    row1 -= 2;
    row2 += 1;
    row3 += 1;

    if (flushMidWay) {
      flushRegionServer();
    }

    // Writes
    BatchUpdate write = new BatchUpdate(ROW1);
    write.put(COL_A, Bytes.toBytes(row1));
    table.commit(transactionState, write);

    write = new BatchUpdate(ROW2);
    write.put(COL_A, Bytes.toBytes(row2));
    table.commit(transactionState, write);

    write = new BatchUpdate(ROW3);
    write.put(COL_A, Bytes.toBytes(row3));
    table.commit(transactionState, write);

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
          Scanner s = t.getScanner(new byte[][] { COL_A },
              HConstants.EMPTY_START_ROW);
          s.close();

        } catch (IOException e) {
          LOG.fatal("could not re-open meta table because", e);
          fail();
        }
        Scanner scanner = null;
        try {
          verify(numRuns);
          LOG.info("Success!");
        } catch (Exception e) {
          e.printStackTrace();
          fail();
        } finally {
          if (scanner != null) {
            LOG.info("Closing scanner " + scanner);
            scanner.close();
          }
        }
      }
    };
    return new Thread(runnable);
  }
}
