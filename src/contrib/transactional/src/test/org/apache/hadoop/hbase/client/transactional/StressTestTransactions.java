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
package org.apache.hadoop.hbase.client.transactional;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;
import org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Stress Test the transaction functionality. This requires to run an
 * {@link TransactionalRegionServer}. We run many threads doing reads/writes
 * which may conflict with each other. We have two types of transactions, those
 * which operate on rows of a single table, and those which operate on rows
 * across multiple tables. Each transaction type has a modification operation
 * which changes two values while maintaining the sum. Also each transaction
 * type has a consistency-check operation which sums all rows and verifies that
 * the sum is as expected.
 */
public class StressTestTransactions extends HBaseClusterTestCase {
  protected static final Log LOG = LogFactory
      .getLog(StressTestTransactions.class);

  private static final int NUM_TABLES = 3;
  private static final int NUM_ST_ROWS = 3;
  private static final int NUM_MT_ROWS = 3;
  private static final int NUM_TRANSACTIONS_PER_THREAD = 100;
  private static final int NUM_SINGLE_TABLE_THREADS = 6;
  private static final int NUM_MULTI_TABLE_THREADS = 6;
  private static final int PRE_COMMIT_SLEEP = 10;
  protected static final Random RAND = new Random();

  private static final byte[] FAMILY_COLON = Bytes.toBytes("family:");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] QUAL_A = Bytes.toBytes("a");
  static final byte[] COL = Bytes.toBytes("family:a");

  private HBaseAdmin admin;
  protected TransactionalTable[] tables;
  protected TransactionManager transactionManager;

  /** constructor */
  public StressTestTransactions() {
    conf.set(HConstants.REGION_SERVER_CLASS, TransactionalRegionInterface.class
        .getName());
    conf.set(HConstants.REGION_SERVER_IMPL, TransactionalRegionServer.class
        .getName());
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    tables = new TransactionalTable[NUM_TABLES];

    for (int i = 0; i < tables.length; i++) {
      HTableDescriptor desc = new HTableDescriptor(makeTableName(i));
      desc.addFamily(new HColumnDescriptor(FAMILY_COLON));
      admin = new HBaseAdmin(conf);
      admin.createTable(desc);
      tables[i] = new TransactionalTable(conf, desc.getName());
    }

    transactionManager = new TransactionManager(conf);
  }

  private String makeTableName(final int i) {
    return "table" + i;
  }

  private void writeInitalValues() throws IOException {
    for (TransactionalTable table : tables) {
      for (int i = 0; i < NUM_ST_ROWS; i++) {
        table.put(new Put(makeSTRow(i)).add(FAMILY, QUAL_A, Bytes
            .toBytes(SingleTableTransactionThread.INITIAL_VALUE)));
      }
      for (int i = 0; i < NUM_MT_ROWS; i++) {
        table.put(new Put(makeMTRow(i)).add(FAMILY, QUAL_A, Bytes
            .toBytes(MultiTableTransactionThread.INITIAL_VALUE)));
      }
    }
  }

  protected byte[] makeSTRow(final int i) {
    return Bytes.toBytes("st" + i);
  }

  protected byte[] makeMTRow(final int i) {
    return Bytes.toBytes("mt" + i);
  }

  static int nextThreadNum = 1;
  protected static final AtomicBoolean stopRequest = new AtomicBoolean(false);
  static final AtomicBoolean consistencyFailure = new AtomicBoolean(false);

  // Thread which runs transactions
  abstract class TransactionThread extends Thread {
    private int numRuns = 0;
    private int numAborts = 0;
    private int numUnknowns = 0;

    public TransactionThread(final String namePrefix) {
      super.setName(namePrefix + "transaction " + nextThreadNum++);
    }

    @Override
    public void run() {
      for (int i = 0; i < NUM_TRANSACTIONS_PER_THREAD; i++) {
        if (stopRequest.get()) {
          return;
        }
        try {
          numRuns++;
          transaction();
        } catch (UnknownTransactionException e) {
          numUnknowns++;
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (CommitUnsuccessfulException e) {
          numAborts++;
        }
      }
    }

    protected abstract void transaction() throws IOException,
        CommitUnsuccessfulException;

    public int getNumAborts() {
      return numAborts;
    }

    public int getNumUnknowns() {
      return numUnknowns;
    }

    protected void preCommitSleep() {
      try {
        Thread.sleep(PRE_COMMIT_SLEEP);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    protected void consistencyFailure() {
      LOG.fatal("Consistency failure");
      stopRequest.set(true);
      consistencyFailure.set(true);
    }

    /**
     * Get the numRuns.
     * 
     * @return Return the numRuns.
     */
    public int getNumRuns() {
      return numRuns;
    }

  }

  // Atomically change the value of two rows rows while maintaining the sum.
  // This should preserve the global sum of the rows, which is also checked
  // with a transaction.
  private class SingleTableTransactionThread extends TransactionThread {
    private static final int INITIAL_VALUE = 10;
    public static final int TOTAL_SUM = INITIAL_VALUE * NUM_ST_ROWS;
    private static final int MAX_TRANSFER_AMT = 100;

    private TransactionalTable table;
    boolean doCheck = false;

    public SingleTableTransactionThread() {
      super("single table ");
    }

    @Override
    protected void transaction() throws IOException,
        CommitUnsuccessfulException {
      if (doCheck) {
        checkTotalSum();
      } else {
        doSingleRowChange();
      }
      doCheck = !doCheck;
    }

    private void doSingleRowChange() throws IOException,
        CommitUnsuccessfulException {
      table = tables[RAND.nextInt(NUM_TABLES)];
      int transferAmount = RAND.nextInt(MAX_TRANSFER_AMT * 2)
          - MAX_TRANSFER_AMT;
      int row1Index = RAND.nextInt(NUM_ST_ROWS);
      int row2Index;
      do {
        row2Index = RAND.nextInt(NUM_ST_ROWS);
      } while (row2Index == row1Index);
      byte[] row1 = makeSTRow(row1Index);
      byte[] row2 = makeSTRow(row2Index);

      TransactionState transactionState = transactionManager.beginTransaction();
      int row1Amount = Bytes.toInt(table.get(transactionState,
          new Get(row1).addColumn(COL)).getValue(COL));
      int row2Amount = Bytes.toInt(table.get(transactionState,
          new Get(row2).addColumn(COL)).getValue(COL));

      row1Amount -= transferAmount;
      row2Amount += transferAmount;

      table.put(transactionState, new Put(row1).add(FAMILY, QUAL_A, Bytes
          .toBytes(row1Amount)));
      table.put(transactionState, new Put(row2).add(FAMILY, QUAL_A, Bytes
          .toBytes(row2Amount)));

      super.preCommitSleep();

      transactionManager.tryCommit(transactionState);
      LOG.debug("Commited");
    }

    // Check the table we last mutated
    private void checkTotalSum() throws IOException,
        CommitUnsuccessfulException {
      TransactionState transactionState = transactionManager.beginTransaction();
      int totalSum = 0;
      for (int i = 0; i < NUM_ST_ROWS; i++) {
        totalSum += Bytes.toInt(table.get(transactionState,
            new Get(makeSTRow(i)).addColumn(COL)).getValue(COL));
      }

      transactionManager.tryCommit(transactionState);
      if (TOTAL_SUM != totalSum) {
        super.consistencyFailure();
      }
    }

  }

  // Similar to SingleTable, but this time we maintain consistency across tables
  // rather than rows
  private class MultiTableTransactionThread extends TransactionThread {
    private static final int INITIAL_VALUE = 1000;
    public static final int TOTAL_SUM = INITIAL_VALUE * NUM_TABLES;
    private static final int MAX_TRANSFER_AMT = 100;

    private byte[] row;
    boolean doCheck = false;

    public MultiTableTransactionThread() {
      super("multi table");
    }

    @Override
    protected void transaction() throws IOException,
        CommitUnsuccessfulException {
      if (doCheck) {
        checkTotalSum();
      } else {
        doSingleRowChange();
      }
      doCheck = !doCheck;
    }

    private void doSingleRowChange() throws IOException,
        CommitUnsuccessfulException {
      row = makeMTRow(RAND.nextInt(NUM_MT_ROWS));
      int transferAmount = RAND.nextInt(MAX_TRANSFER_AMT * 2)
          - MAX_TRANSFER_AMT;
      int table1Index = RAND.nextInt(tables.length);
      int table2Index;
      do {
        table2Index = RAND.nextInt(tables.length);
      } while (table2Index == table1Index);

      TransactionalTable table1 = tables[table1Index];
      TransactionalTable table2 = tables[table2Index];

      TransactionState transactionState = transactionManager.beginTransaction();
      int table1Amount = Bytes.toInt(table1.get(transactionState,
          new Get(row).addColumn(COL)).getValue(COL));
      int table2Amount = Bytes.toInt(table2.get(transactionState,
          new Get(row).addColumn(COL)).getValue(COL));

      table1Amount -= transferAmount;
      table2Amount += transferAmount;

      table1.put(transactionState, new Put(row).add(FAMILY, QUAL_A, Bytes
          .toBytes(table1Amount)));
      table2.put(transactionState, new Put(row).add(FAMILY, QUAL_A, Bytes
          .toBytes(table2Amount)));

      super.preCommitSleep();

      transactionManager.tryCommit(transactionState);

      LOG.trace(Bytes.toString(table1.getTableName()) + ": " + table1Amount);
      LOG.trace(Bytes.toString(table2.getTableName()) + ": " + table2Amount);

    }

    private void checkTotalSum() throws IOException,
        CommitUnsuccessfulException {
      TransactionState transactionState = transactionManager.beginTransaction();
      int totalSum = 0;
      int[] amounts = new int[tables.length];
      for (int i = 0; i < tables.length; i++) {
        int amount = Bytes.toInt(tables[i].get(transactionState,
            new Get(row).addColumn(COL)).getValue(COL));
        amounts[i] = amount;
        totalSum += amount;
      }

      transactionManager.tryCommit(transactionState);

      for (int i = 0; i < tables.length; i++) {
        LOG.trace(Bytes.toString(tables[i].getTableName()) + ": " + amounts[i]);
      }

      if (TOTAL_SUM != totalSum) {
        super.consistencyFailure();
      }
    }

  }

  public void testStressTransactions() throws IOException, InterruptedException {
    writeInitalValues();

    List<TransactionThread> transactionThreads = new LinkedList<TransactionThread>();

    for (int i = 0; i < NUM_SINGLE_TABLE_THREADS; i++) {
      TransactionThread transactionThread = new SingleTableTransactionThread();
      transactionThread.start();
      transactionThreads.add(transactionThread);
    }

    for (int i = 0; i < NUM_MULTI_TABLE_THREADS; i++) {
      TransactionThread transactionThread = new MultiTableTransactionThread();
      transactionThread.start();
      transactionThreads.add(transactionThread);
    }

    for (TransactionThread transactionThread : transactionThreads) {
      transactionThread.join();
    }

    for (TransactionThread transactionThread : transactionThreads) {
      LOG.info(transactionThread.getName() + " done with "
          + transactionThread.getNumAborts() + " aborts, and "
          + transactionThread.getNumUnknowns() + " unknown transactions of "
          + transactionThread.getNumRuns());
    }

    doFinalConsistencyChecks();
  }

  private void doFinalConsistencyChecks() throws IOException {

    int[] mtSums = new int[NUM_MT_ROWS];
    for (int i = 0; i < mtSums.length; i++) {
      mtSums[i] = 0;
    }

    for (TransactionalTable table : tables) {
      int thisTableSum = 0;
      for (int i = 0; i < NUM_ST_ROWS; i++) {
        byte[] row = makeSTRow(i);
        thisTableSum += Bytes.toInt(table.get(new Get(row).addColumn(COL))
            .getValue(COL));
      }
      Assert.assertEquals(SingleTableTransactionThread.TOTAL_SUM, thisTableSum);

      for (int i = 0; i < NUM_MT_ROWS; i++) {
        byte[] row = makeMTRow(i);
        mtSums[i] += Bytes.toInt(table.get(new Get(row).addColumn(COL))
            .getValue(COL));
      }
    }

    for (int mtSum : mtSums) {
      Assert.assertEquals(MultiTableTransactionThread.TOTAL_SUM, mtSum);
    }
  }
}
