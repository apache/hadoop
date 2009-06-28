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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Table with transactional support.
 * 
 */
public class TransactionalTable extends HTable {

  /**
   * @param conf
   * @param tableName
   * @throws IOException
   */
  public TransactionalTable(final HBaseConfiguration conf,
      final String tableName) throws IOException {
    this(conf, Bytes.toBytes(tableName));
  }

  /**
   * @param conf
   * @param tableName
   * @throws IOException
   */
  public TransactionalTable(final HBaseConfiguration conf,
      final byte[] tableName) throws IOException {
    super(conf, tableName);
  }

  private static abstract class TransactionalServerCallable<T> extends
      ServerCallable<T> {
    protected TransactionState transactionState;

    protected TransactionalRegionInterface getTransactionServer() {
      return (TransactionalRegionInterface) server;
    }

    protected void recordServer() throws IOException {
      if (transactionState.addRegion(location)) {
        getTransactionServer().beginTransaction(
            transactionState.getTransactionId(),
            location.getRegionInfo().getRegionName());
      }
    }

    /**
     * @param connection
     * @param tableName
     * @param row
     * @param transactionState
     */
    public TransactionalServerCallable(final HConnection connection,
        final byte[] tableName, final byte[] row,
        final TransactionState transactionState) {
      super(connection, tableName, row);
      this.transactionState = transactionState;
    }

  }
  
  /**
   * Method for getting data from a row
   * @param get the Get to fetch
   * @return the result
   * @throws IOException
   * @since 0.20.0
   */
  public Result get(final TransactionState transactionState, final Get get) throws IOException {
    return super.getConnection().getRegionServerWithRetries(
        new TransactionalServerCallable<Result>(super.getConnection(), super
            .getTableName(), get.getRow(), transactionState) {
          public Result call() throws IOException {
            recordServer();
            return getTransactionServer().get(
                transactionState.getTransactionId(),
                location.getRegionInfo().getRegionName(), get);
          }
        });
  }

  
  /**
   * 
   * @param delete 
   * @throws IOException
   * @since 0.20.0
   */
  public void delete(final TransactionState transactionState, final Delete delete)
  throws IOException {
     super.getConnection().getRegionServerWithRetries(
        new TransactionalServerCallable<Object>(super.getConnection(), super
            .getTableName(), delete.getRow(), transactionState) {
          public Object call() throws IOException {
            recordServer();
             getTransactionServer().delete(
                transactionState.getTransactionId(),
                location.getRegionInfo().getRegionName(), delete);
             return null;
          }
        });
      
  }
  
  /**
   * Commit a Put to the table.
   * <p>
   * If autoFlush is false, the update is buffered.
   * @param put
   * @throws IOException
   * @since 0.20.0
   */
  public synchronized void put(TransactionState transactionState, final Put put) throws IOException {
    //super.validatePut(put);
    super.getConnection().getRegionServerWithRetries(
        new TransactionalServerCallable<Object>(super.getConnection(), super
            .getTableName(), put.getRow(), transactionState) {
          public Object call() throws IOException {
            recordServer();
             getTransactionServer().put(
                transactionState.getTransactionId(),
                location.getRegionInfo().getRegionName(), put);
             return null;
          }
        });
    
  }
  
  public ResultScanner getScanner(final TransactionState transactionState,
     Scan scan) throws IOException {
    ClientScanner scanner = new TransactionalClientScanner(transactionState, scan);
    scanner.initialize();
    return scanner;
  }

  protected class TransactionalClientScanner extends HTable.ClientScanner {

    private TransactionState transactionState;

    protected TransactionalClientScanner(
        final TransactionState transactionState, Scan scan) {
      super(scan);
      this.transactionState = transactionState;
    }

    @Override
    protected ScannerCallable getScannerCallable(
        final byte[] localStartKey, int caching) {
      TransactionScannerCallable t = 
          new TransactionScannerCallable(transactionState, getConnection(),
          getTableName(), getScan().getStartRow(), getScan());
      t.setCaching(caching);
      return t;
    }
  }

}
