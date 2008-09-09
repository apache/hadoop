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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;

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
    super(conf, tableName);
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
   * Get a single value for the specified row and column
   * 
   * @param transactionState
   * @param row row key
   * @param column column name
   * @return value for specified row/column
   * @throws IOException
   */
  public Cell get(final TransactionState transactionState, final byte[] row,
      final byte[] column) throws IOException {
    return super.getConnection().getRegionServerWithRetries(
        new TransactionalServerCallable<Cell>(super.getConnection(), super
            .getTableName(), row, transactionState) {
          public Cell call() throws IOException {
            recordServer();
            return getTransactionServer().get(
                transactionState.getTransactionId(),
                location.getRegionInfo().getRegionName(), row, column);
          }
        });
  }

  /**
   * Get the specified number of versions of the specified row and column
   * 
   * @param transactionState
   * @param row - row key
   * @param column - column name
   * @param numVersions - number of versions to retrieve
   * @return - array byte values
   * @throws IOException
   */
  public Cell[] get(final TransactionState transactionState, final byte[] row,
      final byte[] column, final int numVersions) throws IOException {
    Cell[] values = null;
    values = super.getConnection().getRegionServerWithRetries(
        new TransactionalServerCallable<Cell[]>(super.getConnection(), super
            .getTableName(), row, transactionState) {
          public Cell[] call() throws IOException {
            recordServer();
            return getTransactionServer().get(
                transactionState.getTransactionId(),
                location.getRegionInfo().getRegionName(), row, column,
                numVersions);
          }
        });

    return values;
  }

  /**
   * Get the specified number of versions of the specified row and column with
   * the specified timestamp.
   * 
   * @param transactionState
   * @param row - row key
   * @param column - column name
   * @param timestamp - timestamp
   * @param numVersions - number of versions to retrieve
   * @return - array of values that match the above criteria
   * @throws IOException
   */
  public Cell[] get(final TransactionState transactionState, final byte[] row,
      final byte[] column, final long timestamp, final int numVersions)
      throws IOException {
    Cell[] values = null;
    values = super.getConnection().getRegionServerWithRetries(
        new TransactionalServerCallable<Cell[]>(super.getConnection(), super
            .getTableName(), row, transactionState) {
          public Cell[] call() throws IOException {
            recordServer();
            return getTransactionServer().get(
                transactionState.getTransactionId(),
                location.getRegionInfo().getRegionName(), row, column,
                timestamp, numVersions);
          }
        });

    return values;
  }

  /**
   * Get all the data for the specified row at the latest timestamp
   * 
   * @param transactionState
   * @param row row key
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final TransactionState transactionState,
      final byte[] row) throws IOException {
    return getRow(transactionState, row, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Get all the data for the specified row at a specified timestamp
   * 
   * @param transactionState
   * @param row row key
   * @param ts timestamp
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final TransactionState transactionState,
      final byte[] row, final long ts) throws IOException {
    return super.getConnection().getRegionServerWithRetries(
        new TransactionalServerCallable<RowResult>(super.getConnection(), super
            .getTableName(), row, transactionState) {
          public RowResult call() throws IOException {
            recordServer();
            return getTransactionServer().getRow(
                transactionState.getTransactionId(),
                location.getRegionInfo().getRegionName(), row, ts);
          }
        });
  }

  /**
   * Get selected columns for the specified row at the latest timestamp
   * 
   * @param transactionState
   * @param row row key
   * @param columns Array of column names you want to retrieve.
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final TransactionState transactionState,
      final byte[] row, final byte[][] columns) throws IOException {
    return getRow(transactionState, row, columns, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Get selected columns for the specified row at a specified timestamp
   * 
   * @param transactionState
   * @param row row key
   * @param columns Array of column names you want to retrieve.
   * @param ts timestamp
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final TransactionState transactionState,
      final byte[] row, final byte[][] columns, final long ts)
      throws IOException {
    return super.getConnection().getRegionServerWithRetries(
        new TransactionalServerCallable<RowResult>(super.getConnection(), super
            .getTableName(), row, transactionState) {
          public RowResult call() throws IOException {
            recordServer();
            return getTransactionServer().getRow(
                transactionState.getTransactionId(),
                location.getRegionInfo().getRegionName(), row, columns, ts);
          }
        });
  }

  /**
   * Delete all cells that match the passed row and whose timestamp is equal-to
   * or older than the passed timestamp.
   * 
   * @param transactionState
   * @param row Row to update
   * @param ts Delete all cells of the same timestamp or older.
   * @throws IOException
   */
  public void deleteAll(final TransactionState transactionState,
      final byte[] row, final long ts) throws IOException {
    super.getConnection().getRegionServerWithRetries(
        new TransactionalServerCallable<Boolean>(super.getConnection(), super
            .getTableName(), row, transactionState) {
          public Boolean call() throws IOException {
            recordServer();
            getTransactionServer().deleteAll(
                transactionState.getTransactionId(),
                location.getRegionInfo().getRegionName(), row, ts);
            return null;
          }
        });
  }

  /**
   * Get a scanner on the current table starting at first row. Return the
   * specified columns.
   * 
   * @param transactionState
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned. Its also possible to
   * pass a regex in the column qualifier. A column qualifier is judged to be a
   * regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final TransactionState transactionState,
      final byte[][] columns) throws IOException {
    return getScanner(transactionState, columns, HConstants.EMPTY_START_ROW,
        HConstants.LATEST_TIMESTAMP, null);
  }

  /**
   * Get a scanner on the current table starting at the specified row. Return
   * the specified columns.
   * 
   * @param transactionState
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned. Its also possible to
   * pass a regex in the column qualifier. A column qualifier is judged to be a
   * regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final TransactionState transactionState,
      final byte[][] columns, final byte[] startRow) throws IOException {
    return getScanner(transactionState, columns, startRow,
        HConstants.LATEST_TIMESTAMP, null);
  }

  /**
   * Get a scanner on the current table starting at the specified row. Return
   * the specified columns.
   * 
   * @param transactionState
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned. Its also possible to
   * pass a regex in the column qualifier. A column qualifier is judged to be a
   * regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param timestamp only return results whose timestamp <= this value
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final TransactionState transactionState,
      final byte[][] columns, final byte[] startRow, final long timestamp)
      throws IOException {
    return getScanner(transactionState, columns, startRow, timestamp, null);
  }

  /**
   * Get a scanner on the current table starting at the specified row. Return
   * the specified columns.
   * 
   * @param transactionState
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned. Its also possible to
   * pass a regex in the column qualifier. A column qualifier is judged to be a
   * regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param filter a row filter using row-key regexp and/or column data filter.
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final TransactionState transactionState,
      final byte[][] columns, final byte[] startRow,
      final RowFilterInterface filter) throws IOException {
    return getScanner(transactionState, columns, startRow,
        HConstants.LATEST_TIMESTAMP, filter);
  }

  /**
   * Get a scanner on the current table starting at the specified row. Return
   * the specified columns.
   * 
   * @param transactionState
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned. Its also possible to
   * pass a regex in the column qualifier. A column qualifier is judged to be a
   * regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param timestamp only return results whose timestamp <= this value
   * @param filter a row filter using row-key regexp and/or column data filter.
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final TransactionState transactionState,
      final byte[][] columns, final byte[] startRow, final long timestamp,
      final RowFilterInterface filter) throws IOException {
    ClientScanner scanner = new TransactionalClientScanner(transactionState, columns, startRow,
        timestamp, filter);
    scanner.initialize();
    return scanner;
  }

  /**
   * Commit a BatchUpdate to the table.
   * 
   * @param transactionState
   * @param batchUpdate
   * @throws IOException
   */
  public synchronized void commit(final TransactionState transactionState,
      final BatchUpdate batchUpdate) throws IOException {
    super.getConnection().getRegionServerWithRetries(
        new TransactionalServerCallable<Boolean>(super.getConnection(), super
            .getTableName(), batchUpdate.getRow(), transactionState) {
          public Boolean call() throws IOException {
            recordServer();
            getTransactionServer().batchUpdate(
                transactionState.getTransactionId(),
                location.getRegionInfo().getRegionName(), batchUpdate);
            return null;
          }
        });
  }

  protected class TransactionalClientScanner extends HTable.ClientScanner {

    private TransactionState transactionState;

    protected TransactionalClientScanner(
        final TransactionState transactionState, final byte[][] columns,
        final byte[] startRow, final long timestamp,
        final RowFilterInterface filter) {
      super(columns, startRow, timestamp, filter);
      this.transactionState = transactionState;
    }

    @Override
    protected ScannerCallable getScannerCallable(final byte[] localStartKey) {
      return new TransactionScannerCallable(transactionState, getConnection(),
          getTableName(), getColumns(), localStartKey, getTimestamp(),
          getFilter());
    }
  }

}
