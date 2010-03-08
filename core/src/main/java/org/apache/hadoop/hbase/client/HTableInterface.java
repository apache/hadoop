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
   * Gets the configuration of this instance.
   *
   * @return The configuration.
   */
  Configuration getConfiguration();

  /**
   * Gets the table descriptor for this table.
   *
   * @return table metadata
   * @throws IOException e
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
   * Method for getting data from a row.
   * If the row cannot be found an empty Result is returned.
   * This can be checked by calling {@link Result#isEmpty()}
   *
   * @param get the Get to fetch
   * @return the result
   * @throws IOException e
   */
  Result get(Get get) throws IOException;

  /**
   * Return the row that matches <i>row</i> and <i>family</i> exactly, or the
   * one that immediately precedes it.
   *
   * @param row row key
   * @param family Column family to look for row in
   * @return map of values
   * @throws IOException e
   */
  Result getRowOrBefore(byte[] row, byte[] family) throws IOException;

  /**
   * Get a scanner on the current table as specified by the {@link Scan} object.
   *
   * @param scan a configured {@link Scan} object
   * @return the scanner
   * @throws IOException e
   */
  ResultScanner getScanner(Scan scan) throws IOException;

  /**
   * Get a scanner on the current table as specified by the {@link Scan} object.
   *
   * @param family the column family to scan
   * @return the scanner
   * @throws IOException e
   */
  ResultScanner getScanner(byte[] family) throws IOException;

  /**
   * Get a scanner on the current table as specified by the {@link Scan} object.
   *
   * @param family the column family to scan
   * @param qualifier the column qualifier to scan
   * @return The scanner
   * @throws IOException e
   */
  ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException;

  /**
   * Commit a Put to the table.
   * <p>
   * If autoFlush is false, the update is buffered.
   *
   * @param put data
   * @throws IOException e
   */
  void put(Put put) throws IOException;

  /**
   * Commit a List of Puts to the table.
   * <p>
   * If autoFlush is false, the update is buffered.
   *
   * @param puts list of puts
   * @throws IOException e
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
   * Deletes as specified by the delete.
   *
   * @param delete a delete
   * @throws IOException e
   */
  void delete(Delete delete) throws IOException;

  /**
   * Bulk commit a List of Deletes to the table.
   * @param deletes List of deletes. List is modified by this method.
   * On exception holds deletes that were NOT applied.
   * @throws IOException e
   */
  void delete(List<Delete> deletes) throws IOException;

  /**
   * Atomically increments a column value. If the column value already exists
   * and is not a big-endian long, this could throw an exception. If the column
   * value does not yet exist it is initialized to <code>amount</code> and
   * written to the specified column.
   *
   * @param row row to increment
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @return the new value
   * @throws IOException e
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
   * @param row row to increment
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL true if increment should be applied to WAL, false if not
   * @return The new value.
   * @throws IOException e
   */
  long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, boolean writeToWAL) throws IOException; 

  /**
   * Get the value of autoFlush. If true, updates will not be buffered.
   *
   * @return true if autoFlush is enabled for this table
   */
  boolean isAutoFlush();

  /**
   * Flushes buffer data. Called automatically when autoFlush is true.
   *
   * @throws IOException e
   */
  void flushCommits() throws IOException;

  /**
   * Releases held resources.
   *
   * @throws IOException e
   */
  void close() throws IOException;

  /**
   * Obtains a row lock.
   *
   * @param row the row to lock
   * @return rowLock RowLock containing row and lock id
   * @throws IOException e
   */
  RowLock lockRow(byte[] row) throws IOException;

  /**
   * Releases the row lock.
   *
   * @param rl the row lock to release
   * @throws IOException e
   */
  void unlockRow(RowLock rl) throws IOException;
}
