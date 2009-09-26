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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseBackedTransactionLogger implements TransactionLogger {

  /** The name of the transaction status table. */
  public static final String TABLE_NAME = "__GLOBAL_TRX_LOG__";

  /**
   * Column which holds the transaction status.
   * 
   */
  private static final byte [] STATUS_FAMILY = Bytes.toBytes("Info");
  private static final byte [] STATUS_QUALIFIER = Bytes.toBytes("Status");
  /**
   * Create the table.
   * 
   * @throws IOException
   * 
   */
  public static void createTable() throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(TABLE_NAME);
    tableDesc.addFamily(new HColumnDescriptor(STATUS_FAMILY));
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
      Result result = table.get(new Get(getRow(transactionId)));
      if (result == null || result.isEmpty()) {
        return null;
      }
      byte [] statusValue = result.getValue(STATUS_FAMILY, STATUS_QUALIFIER);
      if (statusValue == null) {
        throw new RuntimeException("No status cell for row " + transactionId);
      }
      String statusString = Bytes.toString(statusValue);
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
    Put put = new Put(getRow(transactionId));
    put.add(STATUS_FAMILY, STATUS_QUALIFIER, Bytes.toBytes(status.name()));
    try {
      table.put(put);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void forgetTransaction(long transactionId) {
    Delete delete = new Delete(getRow(transactionId));
    delete.deleteColumns(STATUS_FAMILY, STATUS_QUALIFIER);
    try {
      table.delete(delete);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
