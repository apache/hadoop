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
package org.apache.hadoop.hbase.client.transactional;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;
import org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test the transaction functionality. This requires to run an
 * {@link TransactionalRegionServer}.
 */
public class TestTransactions extends HBaseClusterTestCase {

  private static final String TABLE_NAME = "table1";

  private static final byte[] FAMILY_COLON = Bytes.toBytes("family:");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] QUAL_A = Bytes.toBytes("a");
  private static final byte[] COL_A = Bytes.toBytes("family:a");

  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] ROW3 = Bytes.toBytes("row3");

  private HBaseAdmin admin;
  private TransactionalTable table;
  private TransactionManager transactionManager;

  /** constructor */
  public TestTransactions() {
    conf.set(HConstants.REGION_SERVER_CLASS, TransactionalRegionInterface.class
        .getName());
    conf.set(HConstants.REGION_SERVER_IMPL, TransactionalRegionServer.class
        .getName());
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(FAMILY_COLON));
    admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    table = new TransactionalTable(conf, desc.getName());

    transactionManager = new TransactionManager(conf);
    writeInitalRow();
  }

  private void writeInitalRow() throws IOException {
    table.put(new Put(ROW1).add(FAMILY, QUAL_A, Bytes.toBytes(1)));
  }

  public void testSimpleTransaction() throws IOException,
      CommitUnsuccessfulException {
    TransactionState transactionState = makeTransaction1();
    transactionManager.tryCommit(transactionState);
  }

  public void testTwoTransactionsWithoutConflict() throws IOException,
      CommitUnsuccessfulException {
    TransactionState transactionState1 = makeTransaction1();
    TransactionState transactionState2 = makeTransaction2();

    transactionManager.tryCommit(transactionState1);
    transactionManager.tryCommit(transactionState2);
  }

  public void TestTwoTransactionsWithConflict() throws IOException,
      CommitUnsuccessfulException {
    TransactionState transactionState1 = makeTransaction1();
    TransactionState transactionState2 = makeTransaction2();

    transactionManager.tryCommit(transactionState2);

    try {
      transactionManager.tryCommit(transactionState1);
      fail();
    } catch (CommitUnsuccessfulException e) {
      // Good
    }
  }

  // Read from ROW1,COL_A and put it in ROW2_COLA and ROW3_COLA
  private TransactionState makeTransaction1() throws IOException {
    TransactionState transactionState = transactionManager.beginTransaction();

    Result row1_A = table.get(transactionState, new Get(ROW1).addColumn(COL_A));

    table.put(new Put(ROW2).add(FAMILY, QUAL_A, row1_A.getValue(COL_A)));
    table.put(new Put(ROW3).add(FAMILY, QUAL_A, row1_A.getValue(COL_A)));

    return transactionState;
  }

  // Read ROW1,COL_A, increment its (integer) value, write back
  private TransactionState makeTransaction2() throws IOException {
    TransactionState transactionState = transactionManager.beginTransaction();

    Result row1_A = table.get(transactionState, new Get(ROW1).addColumn(COL_A));

    int value = Bytes.toInt(row1_A.getValue(COL_A));

    table.put(new Put(ROW1).add(FAMILY, QUAL_A, Bytes.toBytes(value + 1)));

    return transactionState;
  }
}
