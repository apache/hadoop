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
import java.util.Random;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseBackedTransactionLogger implements TransactionLogger {

  /** The name of the transaction status table. */
  public static final String TABLE_NAME = "__GLOBAL_TRX_LOG__";

  private static final String INFO_FAMILY = "Info:";

  /**
   * Column which holds the transaction status.
   * 
   */
  private static final String STATUS_COLUMN = INFO_FAMILY + "Status";
  private static final byte[] STATUS_COLUMN_BYTES = Bytes
      .toBytes(STATUS_COLUMN);

  /**
   * Create the table.
   * 
   * @throws IOException
   * 
   */
  public static void createTable() throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(TABLE_NAME);
    tableDesc.addFamily(new HColumnDescriptor(INFO_FAMILY));
    HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
    admin.createTable(tableDesc);
  }

  private Random random = new Random();
  private HTable table;

  public HBaseBackedTransactionLogger() throws IOException {
    initTable();
  }

  private void initTable() throws IOException {
    HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());

    if (!admin.tableExists(TABLE_NAME)) {
      throw new RuntimeException("Table not created. Call createTable() first");
    }
    this.table = new HTable(TABLE_NAME);

  }

  public long createNewTransactionLog() {
    long id;
    TransactionStatus existing;

    do {
      id = random.nextLong();
      existing = getStatusForTransaction(id);
    } while (existing != null);
    
    setStatusForTransaction(id, TransactionStatus.PENDING);

    return id;
  }

  public TransactionStatus getStatusForTransaction(long transactionId) {
    try {
      RowResult result = table.getRow(getRow(transactionId));
      if (result == null || result.isEmpty()) {
        return null;
      }
      Cell statusCell = result.get(STATUS_COLUMN_BYTES);
      if (statusCell == null) {
        throw new RuntimeException("No status cell for row " + transactionId);
      }
      String statusString = Bytes.toString(statusCell.getValue());
      return TransactionStatus.valueOf(statusString);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private byte [] getRow(long transactionId) {
    return Bytes.toBytes(""+transactionId);
  }

  public void setStatusForTransaction(long transactionId,
      TransactionStatus status) {
    BatchUpdate update = new BatchUpdate(getRow(transactionId));
    update.put(STATUS_COLUMN, Bytes.toBytes(status.name()));

    try {
      table.commit(update);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void forgetTransaction(long transactionId) {
    BatchUpdate update = new BatchUpdate(getRow(transactionId));
    update.delete(STATUS_COLUMN);

    try {
      table.commit(update);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
