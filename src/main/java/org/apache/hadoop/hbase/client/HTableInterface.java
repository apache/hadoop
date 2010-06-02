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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.io.IOException;
import java.util.List;

/**
 * Used to communicate with a single HBase table.
 *
 * @since 0.21.0
 */
public interface HTableInterface {

  /**
   * Gets the name of this table.
   *
   * @return the table name.
   */
  byte[] getTableName();

  /**
   * Returns the {@link Configuration} object used by this instance.
   * <p>
   * The reference returned is not a copy, so any change made to it will
   * affect this instance.
   */
  Configuration getConfiguration();

  /**
   * Gets the {@link HTableDescriptor table descriptor} for this table.
   * @throws IOException if a remote or network exception occurs.
   */
  HTableDescriptor getTableDescriptor() throws IOException;

  /**
   * Test for the existence of columns in the table, as specified in the Get.
   * <p>
   *
   * This will return true if the Get matches one or more keys, false if not.
   * <p>
   *
   * This is a server-side call so it prevents any data from being transfered to
   * the client.
   *
   * @param get the Get
   * @return true if the specified Get matches one or more keys, false if not
   * @throws IOException e
   */
  boolean exists(Get get) throws IOException;

  /**
   * Extracts certain cells from a given row.
   * @param get The object that specifies what data to fetch and from which row.
   * @return The data coming from the specified row, if it exists.  If the row
   * specified doesn't exist, the {@link Result} instance returned won't
   * contain any {@link KeyValue}, as indicated by {@link Result#isEmpty()}.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  Result get(Get get) throws IOException;

  /**
   * Return the row that matches <i>row</i> exactly,
   * or the one that immediately precedes it.
   *
   * @param row A row key.
   * @param family Column family to include in the {@link Result}.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  Result getRowOrBefore(byte[] row, byte[] family) throws IOException;

  /**
   * Returns a scanner on the current table as specified by the {@link Scan}
   * object.
   *
   * @param scan A configured {@link Scan} object.
   * @return A scanner.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  ResultScanner getScanner(Scan scan) throws IOException;

  /**
   * Gets a scanner on the current table for the given family.
   *
   * @param family The column family to scan.
   * @return A scanner.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  ResultScanner getScanner(byte[] family) throws IOException;

  /**
   * Gets a scanner on the current table for the given family and qualifier.
   *
   * @param family The column family to scan.
   * @param qualifier The column qualifier to scan.
   * @return A scanner.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException;


  /**
   * Puts some data in the table.
   * <p>
   * If {@link #isAutoFlush isAutoFlush} is false, the update is buffered
   * until the internal buffer is full.
   * @param put The data to put.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  void put(Put put) throws IOException;

  /**
   * Puts some data in the table, in batch.
   * <p>
   * If {@link #isAutoFlush isAutoFlush} is false, the update is buffered
   * until the internal buffer is full.
   * @param puts The list of mutations to apply.  The list gets modified by this
   * method (in particular it gets re-ordered, so the order in which the elements
   * are inserted in the list gives no guarantee as to the order in which the
   * {@link Put}s are executed).
   * @throws IOException if a remote or network exception occurs. In that case
   * the {@code puts} argument will contain the {@link Put} instances that
   * have not be successfully applied.
   * @since 0.20.0
   */
  void put(List<Put> puts) throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the put.  If the passed value is null, the check
   * is for the lack of column (ie: non-existance)
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param value the expected value
   * @param put data to put if check succeeds
   * @throws IOException e
   * @return true if the new put was executed, false otherwise
   */
  boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) throws IOException;

  /**
   * Deletes the specified cells/row.
   *
   * @param delete The object that specifies what to delete.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  void delete(Delete delete) throws IOException;

  /**
   * Deletes the specified cells/rows in bulk.
   * @param deletes List of things to delete.  List gets modified by this
   * method (in particular it gets re-ordered, so the order in which the elements
   * are inserted in the list gives no guarantee as to the order in which the
   * {@link Delete}s are executed).
   * @throws IOException if a remote or network exception occurs. In that case
   * the {@code deletes} argument will contain the {@link Delete} instances
   * that have not be successfully applied.
   * @since 0.20.1
   */
  void delete(List<Delete> deletes) throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the delete.  If the passed value is null, the 
   * check is for the lack of column (ie: non-existance)
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param value the expected value
   * @param delete data to delete if check succeeds
   * @throws IOException e
   * @return true if the new delete was executed, false otherwise
   */
  boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) throws IOException;

  /**
   * Atomically increments a column value.
   * <p>
   * Equivalent to {@code {@link #incrementColumnValue(byte[], byte[], byte[],
   * long, boolean) incrementColumnValue}(row, family, qualifier, amount,
   * <b>true</b>)}
   * @param row The row that contains the cell to increment.
   * @param family The column family of the cell to increment.
   * @param qualifier The column qualifier of the cell to increment.
   * @param amount The amount to increment the cell with (or decrement, if the
   * amount is negative).
   * @return The new value, post increment.
   * @throws IOException if a remote or network exception occurs.
   */
  long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount) throws IOException;

  /**
   * Atomically increments a column value. If the column value already exists
   * and is not a big-endian long, this could throw an exception. If the column
   * value does not yet exist it is initialized to <code>amount</code> and
   * written to the specified column.
   *
   * <p>Setting writeToWAL to false means that in a fail scenario, you will lose
   * any increments that have not been flushed.
   * @param row The row that contains the cell to increment.
   * @param family The column family of the cell to increment.
   * @param qualifier The column qualifier of the cell to increment.
   * @param amount The amount to increment the cell with (or decrement, if the
   * amount is negative).
   * @param writeToWAL if {@code true}, the operation will be applied to the
   * Write Ahead Log (WAL).  This makes the operation slower but safer, as if
   * the call returns successfully, it is guaranteed that the increment will
   * be safely persisted.  When set to {@code false}, the call may return
   * successfully before the increment is safely persisted, so it's possible
   * that the increment be lost in the event of a failure happening before the
   * operation gets persisted.
   * @return The new value, post increment.
   * @throws IOException if a remote or network exception occurs.
   */
  long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, boolean writeToWAL) throws IOException;

  /**
   * Tells whether or not 'auto-flush' is turned on.
   *
   * @return {@code true} if 'auto-flush' is enabled (default), meaning
   * {@link Put} operations don't get buffered/delayed and are immediately
   * executed.
   */
  boolean isAutoFlush();

  /**
   * Executes all the buffered {@link Put} operations.
   * <p>
   * This method gets called once automatically for every {@link Put} or batch
   * of {@link Put}s (when {@link #put(List<Put>)} is used) when
   * {@link #isAutoFlush} is {@code true}.
   * @throws IOException if a remote or network exception occurs.
   */
  void flushCommits() throws IOException;

  /**
   * Releases any resources help or pending changes in internal buffers.
   *
   * @throws IOException if a remote or network exception occurs.
   */
  void close() throws IOException;

  /**
   * Obtains a lock on a row.
   *
   * @param row The row to lock.
   * @return A {@link RowLock} containing the row and lock id.
   * @throws IOException if a remote or network exception occurs.
   * @see RowLock
   * @see #unlockRow
   */
  RowLock lockRow(byte[] row) throws IOException;

  /**
   * Releases a row lock.
   *
   * @param rl The row lock to release.
   * @throws IOException if a remote or network exception occurs.
   * @see RowLock
   * @see #unlockRow
   */
  void unlockRow(RowLock rl) throws IOException;
}
