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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.util.Map;

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
   * Method that does a batch call on Deletes, Gets and Puts.
   *
   * @param actions list of Get, Put, Delete objects
   * @param results Empty Object[], same size as actions. Provides access to partial
   *                results, in case an exception is thrown. A null in the result array means that
   *                the call for that action failed, even after retries
   * @throws IOException
   * @since 0.90.0
   */
  void batch(final List<Row> actions, final Object[] results) throws IOException, InterruptedException;

  /**
   * Method that does a batch call on Deletes, Gets and Puts.
   *
   *
   * @param actions list of Get, Put, Delete objects
   * @return the results from the actions. A null in the return array means that
   *         the call for that action failed, even after retries
   * @throws IOException
   * @since 0.90.0
   */
  Object[] batch(final List<Row> actions) throws IOException, InterruptedException;

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
   * Extracts certain cells from the given rows, in batch.
   *
   * @param gets The objects that specify what data to fetch and from which rows.
   *
   * @return The data coming from the specified rows, if it exists.  If the row
   *         specified doesn't exist, the {@link Result} instance returned won't
   *         contain any {@link KeyValue}, as indicated by {@link Result#isEmpty()}.
   *         If there are any failures even after retries, there will be a null in
   *         the results array for those Gets, AND an exception will be thrown.
   * @throws IOException if a remote or network exception occurs.
   *
   * @since 0.90.0
   */
  Result[] get(List<Get> gets) throws IOException;

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
   * @param puts The list of mutations to apply. The batch put is done by
   * aggregating the iteration of the Puts over the write buffer
   * at the client-side for a single RPC call.
   * @throws IOException if a remote or network exception occurs.
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
   * Increments one or more columns within a single row.
   * <p>
   * This operation does not appear atomic to readers.  Increments are done
   * under a single row lock, so write operations to a row are synchronized, but
   * readers do not take row locks so get and scan operations can see this
   * operation partially completed.
   *
   * @param increment object that specifies the columns and amounts to be used
   *                  for the increment operations
   * @throws IOException e
   * @return values of columns after the increment
   */
  public Result increment(final Increment increment) throws IOException;

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
   * of {@link Put}s (when <code>put(List<Put>)</code> is used) when
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

  /**
   * Creates and returns a proxy to the CoprocessorProtocol instance running in the
   * region containing the specified row.  The row given does not actually have
   * to exist.  Whichever region would contain the row based on start and end keys will
   * be used.  Note that the {@code row} parameter is also not passed to the
   * coprocessor handler registered for this protocol, unless the {@code row}
   * is separately passed as an argument in a proxy method call.  The parameter
   * here is just used to locate the region used to handle the call.
   *
   * @param protocol The class or interface defining the remote protocol
   * @param row The row key used to identify the remote region location
   * @return
   */
  <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol, byte[] row);

  /**
   * Invoke the passed
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call} against
   * the {@link CoprocessorProtocol} instances running in the selected regions.
   * All regions beginning with the region containing the <code>startKey</code>
   * row, through to the region containing the <code>endKey</code> row (inclusive)
   * will be used.  If <code>startKey</code> or <code>endKey</code> is
   * <code>null</code>, the first and last regions in the table, respectively,
   * will be used in the range selection.
   *
   * @param protocol the CoprocessorProtocol implementation to call
   * @param startKey start region selection with region containing this row
   * @param endKey select regions up to and including the region containing
   * this row
   * @param callable wraps the CoprocessorProtocol implementation method calls
   * made per-region
   * @param <T> CoprocessorProtocol subclass for the remote invocation
   * @param <R> Return type for the
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)}
   * method
   * @return a <code>Map</code> of region names to
   * {@link Batch.Call#call(Object)} return values
   */
  <T extends CoprocessorProtocol, R> Map<byte[],R> coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey, Batch.Call<T,R> callable)
      throws IOException, Throwable;

  /**
   * Invoke the passed
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call} against
   * the {@link CoprocessorProtocol} instances running in the selected regions.
   * All regions beginning with the region containing the <code>startKey</code>
   * row, through to the region containing the <code>endKey</code> row
   * (inclusive)
   * will be used.  If <code>startKey</code> or <code>endKey</code> is
   * <code>null</code>, the first and last regions in the table, respectively,
   * will be used in the range selection.
   *
   * <p>
   * For each result, the given
   * {@link Batch.Callback#update(byte[], byte[], Object)}
   * method will be called.
   *</p>
   *
   * @param protocol the CoprocessorProtocol implementation to call
   * @param startKey start region selection with region containing this row
   * @param endKey select regions up to and including the region containing
   * this row
   * @param callable wraps the CoprocessorProtocol implementation method calls
   * made per-region
   * @param callback an instance upon which
   * {@link Batch.Callback#update(byte[], byte[], Object)} with the
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)}
   * return value for each region
   * @param <T> CoprocessorProtocol subclass for the remote invocation
   * @param <R> Return type for the
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)}
   * method
   */
  <T extends CoprocessorProtocol, R> void coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T,R> callable, Batch.Callback<R> callback)
      throws IOException, Throwable;
}
