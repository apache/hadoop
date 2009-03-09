/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.io.HbaseMapWritable;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;

/**
 * Clients interact with HRegionServers using a handle to the HRegionInterface.
 * 
 * <p>NOTE: if you change the interface, you must change the RPC version
 * number in HBaseRPCProtocolVersion
 * 
 */
public interface HRegionInterface extends HBaseRPCProtocolVersion {
  /** 
   * Get metainfo about an HRegion
   * 
   * @param regionName name of the region
   * @return HRegionInfo object for region
   * @throws NotServingRegionException
   */
  public HRegionInfo getRegionInfo(final byte [] regionName)
  throws NotServingRegionException;
  
  /**
   * Get the specified number of versions of the specified row and column with
   * the specified timestamp.
   *
   * @param regionName region name
   * @param row row key
   * @param column column key
   * @param timestamp timestamp
   * @param numVersions number of versions to return
   * @return array of values
   * @throws IOException
   */
  public Cell[] get(final byte [] regionName, final byte [] row,
    final byte [] column, final long timestamp, final int numVersions)
  throws IOException;

  /**
   * Return all the data for the row that matches <i>row</i> exactly, 
   * or the one that immediately preceeds it.
   * 
   * @param regionName region name
   * @param row row key
   * @param columnFamily Column family to look for row in.
   * @return map of values
   * @throws IOException
   */
  public RowResult getClosestRowBefore(final byte [] regionName,
    final byte [] row, final byte [] columnFamily)
  throws IOException;

  /**
   * Get selected columns for the specified row at a given timestamp.
   * 
   * @param regionName region name
   * @param row row key
   * @param columns columns to get
   * @param ts time stamp
   * @param numVersions number of versions
   * @param lockId lock id
   * @return map of values
   * @throws IOException
   */
  public RowResult getRow(final byte [] regionName, final byte [] row, 
    final byte[][] columns, final long ts,
    final int numVersions, final long lockId)
  throws IOException;

  /**
   * Applies a batch of updates via one RPC
   * 
   * @param regionName name of the region to update
   * @param b BatchUpdate
   * @param lockId lock id
   * @throws IOException
   */
  public void batchUpdate(final byte [] regionName, final BatchUpdate b,
      final long lockId)
  throws IOException;
  
  /**
   * Applies a batch of updates via one RPC for many rows
   * 
   * @param regionName name of the region to update
   * @param b BatchUpdate[]
   * @throws IOException
   * @return number of updates applied
   */
  public int batchUpdates(final byte[] regionName, final BatchUpdate[] b)
  throws IOException;
  
  /**
   * Applies a batch of updates to one row atomically via one RPC
   * if the columns specified in expectedValues match
   * the given values in expectedValues
   * 
   * @param regionName name of the region to update
   * @param b BatchUpdate
   * @param expectedValues map of column names to expected data values.
   * @return true if update was applied
   * @throws IOException
   */
  public boolean checkAndSave(final byte [] regionName, final BatchUpdate b,
      final HbaseMapWritable<byte[],byte[]> expectedValues)
  throws IOException;
  

  /**
   * Delete all cells that match the passed row and column and whose timestamp
   * is equal-to or older than the passed timestamp.
   * 
   * @param regionName region name
   * @param row row key
   * @param column column key
   * @param timestamp Delete all entries that have this timestamp or older
   * @param lockId lock id
   * @throws IOException
   */
  public void deleteAll(byte [] regionName, byte [] row, byte [] column,
    long timestamp, long lockId)
  throws IOException;

  /**
   * Delete all cells that match the passed row and whose
   * timestamp is equal-to or older than the passed timestamp.
   *
   * @param regionName region name
   * @param row row key
   * @param timestamp Delete all entries that have this timestamp or older
   * @param lockId lock id
   * @throws IOException
   */
  public void deleteAll(byte [] regionName, byte [] row, long timestamp,
      long lockId)
  throws IOException;
  
  /**
   * Delete all cells that match the passed row & the column regex and whose
   * timestamp is equal-to or older than the passed timestamp.
   * 
   * @param regionName
   * @param row
   * @param colRegex
   * @param timestamp
   * @param lockId
   * @throws IOException
   */
  public void deleteAllByRegex(byte [] regionName, byte [] row, String colRegex, 
      long timestamp, long lockId)
  throws IOException;

  /**
   * Delete all cells for a row with matching column family with timestamps
   * less than or equal to <i>timestamp</i>.
   *
   * @param regionName The name of the region to operate on
   * @param row The row to operate on
   * @param family The column family to match
   * @param timestamp Timestamp to match
   * @param lockId lock id
   * @throws IOException
   */
  public void deleteFamily(byte [] regionName, byte [] row, byte [] family, 
    long timestamp, long lockId)
  throws IOException;
  
  /**
   * Delete all cells for a row with matching column family regex with 
   * timestamps less than or equal to <i>timestamp</i>.
   * 
   * @param regionName The name of the region to operate on
   * @param row The row to operate on
   * @param familyRegex column family regex
   * @param timestamp Timestamp to match
   * @param lockId lock id
   * @throws IOException
   */
  public void deleteFamilyByRegex(byte [] regionName, byte [] row, String familyRegex, 
    long timestamp, long lockId) 
  throws IOException;

  /**
   * Returns true if any cells exist for the given coordinate.
   * 
   * @param regionName The name of the region
   * @param row The row
   * @param column The column, or null for any
   * @param timestamp The timestamp, or LATEST_TIMESTAMP for any
   * @param lockID lock id
   * @return true if the row exists, false otherwise
   * @throws IOException
   */
  public boolean exists(byte [] regionName, byte [] row, byte [] column, 
    long timestamp, long lockID)
  throws IOException;

  //
  // remote scanner interface
  //

  /**
   * Opens a remote scanner with a RowFilter.
   * 
   * @param regionName name of region to scan
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex for column family name. A column name is judged to be
   * regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row to scan
   * @param timestamp only return values whose timestamp is <= this value
   * @param filter RowFilter for filtering results at the row-level.
   *
   * @return scannerId scanner identifier used in other calls
   * @throws IOException
   */
  public long openScanner(final byte [] regionName, final byte [][] columns,
      final byte [] startRow, long timestamp, RowFilterInterface filter)
  throws IOException;
  
  /**
   * Get the next set of values
   * @param scannerId clientId passed to openScanner
   * @return map of values
   * @throws IOException
   */
  public RowResult next(long scannerId) throws IOException;
  
  /**
   * Get the next set of values
   * @param scannerId clientId passed to openScanner
   * @param numberOfRows the number of rows to fetch
   * @return map of values
   * @throws IOException
   */
  public RowResult[] next(long scannerId, int numberOfRows) throws IOException;
  
  /**
   * Close a scanner
   * 
   * @param scannerId the scanner id returned by openScanner
   * @throws IOException
   */
  public void close(long scannerId) throws IOException;
  
  /**
   * Opens a remote row lock.
   *
   * @param regionName name of region
   * @param row row to lock
   * @return lockId lock identifier
   * @throws IOException
   */
  public long lockRow(final byte [] regionName, final byte [] row)
  throws IOException;

  /**
   * Releases a remote row lock.
   *
   * @param regionName
   * @param lockId the lock id returned by lockRow
   * @throws IOException
   */
  public void unlockRow(final byte [] regionName, final long lockId)
  throws IOException;
  
  /**
   * Atomically increments a column value. If the column value isn't long-like, this could
   * throw an exception.
   * 
   * @param regionName
   * @param row
   * @param column
   * @param amount
   * @return new incremented column value
   * @throws IOException
   */
  public long incrementColumnValue(byte [] regionName, byte [] row,
      byte [] column, long amount) throws IOException;
}
