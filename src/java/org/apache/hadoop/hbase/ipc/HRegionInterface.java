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

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;

/**
 * Clients interact with HRegionServers using a handle to the HRegionInterface.
 */
public interface HRegionInterface extends VersionedProtocol {
  /**
   * Protocol version.
   * Upped to 3 when we went from Text to byte arrays for row and column names.
   */
  public static final long versionID = 3L;

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
   * Retrieve a single value from the specified region for the specified row
   * and column keys
   * 
   * @param regionName name of region
   * @param row row key
   * @param column column key
   * @return alue for that region/row/column
   * @throws IOException
   */
  public Cell get(final byte [] regionName, final byte [] row, final byte [] column)
  throws IOException;

  /**
   * Get the specified number of versions of the specified row and column
   * 
   * @param regionName region name
   * @param row row key
   * @param column column key
   * @param numVersions number of versions to return
   * @return array of values
   * @throws IOException
   */
  public Cell[] get(final byte [] regionName, final byte [] row,
    final byte [] column, final int numVersions)
  throws IOException;
  
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
   * @return map of values
   * @throws IOException
   */
  public RowResult getClosestRowBefore(final byte [] regionName,
    final byte [] row)
  throws IOException;

  /**
   * Get selected columns for the specified row at a given timestamp.
   * 
   * @param regionName region name
   * @param row row key
   * @return map of values
   * @throws IOException
   */
  public RowResult getRow(final byte [] regionName, final byte [] row, 
    final byte[][] columns, final long ts)
  throws IOException;

  /**
   * Applies a batch of updates via one RPC
   * 
   * @param regionName name of the region to update
   * @param b BatchUpdate
   * @throws IOException
   */
  public void batchUpdate(final byte [] regionName, final BatchUpdate b)
  throws IOException;
  
  /**
   * Delete all cells that match the passed row and column and whose
   * timestamp is equal-to or older than the passed timestamp.
   *
   * @param regionName region name
   * @param row row key
   * @param column column key
   * @param timestamp Delete all entries that have this timestamp or older
   * @throws IOException
   */
  public void deleteAll(byte [] regionName, byte [] row, byte [] column,
    long timestamp)
  throws IOException;

  /**
   * Delete all cells that match the passed row and whose
   * timestamp is equal-to or older than the passed timestamp.
   *
   * @param regionName region name
   * @param row row key
   * @param timestamp Delete all entries that have this timestamp or older
   * @throws IOException
   */
  public void deleteAll(byte [] regionName, byte [] row, long timestamp)
  throws IOException;

  /**
   * Delete all cells for a row with matching column family with timestamps
   * less than or equal to <i>timestamp</i>.
   *
   * @param regionName The name of the region to operate on
   * @param row The row to operate on
   * @param family The column family to match
   * @param timestamp Timestamp to match
   */
  public void deleteFamily(byte [] regionName, byte [] row, byte [] family, 
    long timestamp)
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
   * Close a scanner
   * 
   * @param scannerId the scanner id returned by openScanner
   * @throws IOException
   */
  public void close(long scannerId) throws IOException;
}