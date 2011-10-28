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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;

/**
 * Clients interact with HRegionServers using a handle to the HRegionInterface.
 *
 * <p>NOTE: if you change the interface, you must change the RPC version
 * number in HBaseRPCProtocolVersion
 */
public interface HRegionInterface extends VersionedProtocol, Stoppable, Abortable {
  /**
   * This Interfaces' version. Version changes when the Interface changes.
   */
  // All HBase Interfaces used derive from HBaseRPCProtocolVersion.  It
  // maintained a single global version number on all HBase Interfaces.  This
  // meant all HBase RPC was broke though only one of the three RPC Interfaces
  // had changed.  This has since been undone.
  public static final long VERSION = 28L;

  /**
   * Get metainfo about an HRegion
   *
   * @param regionName name of the region
   * @return HRegionInfo object for region
   * @throws NotServingRegionException
   * @throws ConnectException
   * @throws IOException This can manifest as an Hadoop ipc {@link RemoteException}
   */
  public HRegionInfo getRegionInfo(final byte [] regionName)
  throws NotServingRegionException, ConnectException, IOException;

  /**
   * Return all the data for the row that matches <i>row</i> exactly,
   * or the one that immediately preceeds it.
   *
   * @param regionName region name
   * @param row row key
   * @param family Column family to look for row in.
   * @return map of values
   * @throws IOException e
   */
  public Result getClosestRowBefore(final byte [] regionName,
    final byte [] row, final byte [] family)
  throws IOException;

  /**
   * Perform Get operation.
   * @param regionName name of region to get from
   * @param get Get operation
   * @return Result
   * @throws IOException e
   */
  public Result get(byte [] regionName, Get get) throws IOException;

  /**
   * Perform exists operation.
   * @param regionName name of region to get from
   * @param get Get operation describing cell to test
   * @return true if exists
   * @throws IOException e
   */
  public boolean exists(byte [] regionName, Get get) throws IOException;

  /**
   * Put data into the specified region
   * @param regionName region name
   * @param put the data to be put
   * @throws IOException e
   */
  public void put(final byte [] regionName, final Put put)
  throws IOException;

  /**
   * Put an array of puts into the specified region
   *
   * @param regionName region name
   * @param puts List of puts to execute
   * @return The number of processed put's.  Returns -1 if all Puts
   * processed successfully.
   * @throws IOException e
   */
  public int put(final byte[] regionName, final List<Put> puts)
  throws IOException;

  /**
   * Deletes all the KeyValues that match those found in the Delete object,
   * if their ts <= to the Delete. In case of a delete with a specific ts it
   * only deletes that specific KeyValue.
   * @param regionName region name
   * @param delete delete object
   * @throws IOException e
   */
  public void delete(final byte[] regionName, final Delete delete)
  throws IOException;

  /**
   * Put an array of deletes into the specified region
   *
   * @param regionName region name
   * @param deletes delete List to execute
   * @return The number of processed deletes.  Returns -1 if all Deletes
   * processed successfully.
   * @throws IOException e
   */
  public int delete(final byte[] regionName, final List<Delete> deletes)
  throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value match the expectedValue.
   * If it does, it adds the put. If passed expected value is null, then the
   * check is for non-existance of the row/column.
   *
   * @param regionName region name
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param put data to put if check succeeds
   * @throws IOException e
   * @return true if the new put was execute, false otherwise
   */
  public boolean checkAndPut(final byte[] regionName, final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put)
  throws IOException;


  /**
   * Atomically checks if a row/family/qualifier value match the expectedValue.
   * If it does, it adds the delete. If passed expected value is null, then the
   * check is for non-existance of the row/column.
   *
   * @param regionName region name
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param delete data to delete if check succeeds
   * @throws IOException e
   * @return true if the new delete was execute, false otherwise
   */
  public boolean checkAndDelete(final byte[] regionName, final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Delete delete)
  throws IOException;

  /**
   * Atomically increments a column value. If the column value isn't long-like,
   * this could throw an exception. If passed expected value is null, then the
   * check is for non-existance of the row/column.
   *
   * @param regionName region name
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL whether to write the increment to the WAL
   * @return new incremented column value
   * @throws IOException e
   */
  public long incrementColumnValue(byte [] regionName, byte [] row,
      byte [] family, byte [] qualifier, long amount, boolean writeToWAL)
  throws IOException;

  /**
   * Increments one or more columns values in a row.  Returns the
   * updated keys after the increment.
   * <p>
   * This operation does not appear atomic to readers.  Increments are done
   * under a row lock but readers do not take row locks.
   * @param regionName region name
   * @param increment increment operation
   * @return incremented cells
   */
  public Result increment(byte[] regionName, Increment increment)
  throws IOException;

  //
  // remote scanner interface
  //

  /**
   * Opens a remote scanner with a RowFilter.
   *
   * @param regionName name of region to scan
   * @param scan configured scan object
   * @return scannerId scanner identifier used in other calls
   * @throws IOException e
   */
  public long openScanner(final byte [] regionName, final Scan scan)
  throws IOException;

  /**
   * Get the next set of values
   * @param scannerId clientId passed to openScanner
   * @return map of values; returns null if no results.
   * @throws IOException e
   */
  public Result next(long scannerId) throws IOException;

  /**
   * Get the next set of values
   * @param scannerId clientId passed to openScanner
   * @param numberOfRows the number of rows to fetch
   * @return Array of Results (map of values); array is empty if done with this
   * region and null if we are NOT to go to the next region (happens when a
   * filter rules that the scan is done).
   * @throws IOException e
   */
  public Result [] next(long scannerId, int numberOfRows) throws IOException;

  /**
   * Close a scanner
   *
   * @param scannerId the scanner id returned by openScanner
   * @throws IOException e
   */
  public void close(long scannerId) throws IOException;

  /**
   * Opens a remote row lock.
   *
   * @param regionName name of region
   * @param row row to lock
   * @return lockId lock identifier
   * @throws IOException e
   */
  public long lockRow(final byte [] regionName, final byte [] row)
  throws IOException;

  /**
   * Releases a remote row lock.
   *
   * @param regionName region name
   * @param lockId the lock id returned by lockRow
   * @throws IOException e
   */
  public void unlockRow(final byte [] regionName, final long lockId)
  throws IOException;


  /**
   * @return All regions online on this region server
   * @throws IOException e
   */
  public List<HRegionInfo> getOnlineRegions() throws IOException;

  /**
   * Method used when a master is taking the place of another failed one.
   * @return This servers {@link HServerInfo}; it has RegionServer POV on the
   * hostname which may not agree w/ how the Master sees this server.
   * @throws IOException e
   * @deprecated
   */
  public HServerInfo getHServerInfo() throws IOException;

  /**
   * Method used for doing multiple actions(Deletes, Gets and Puts) in one call
   * @param multi
   * @return MultiResult
   * @throws IOException
   */
  public <R> MultiResponse multi(MultiAction<R> multi) throws IOException;

  /**
   * Bulk load an HFile into an open region
   */
  public void bulkLoadHFile(String hfilePath, byte[] regionName, byte[] familyName)
  throws IOException;

  // Master methods

  /**
   * Opens the specified region.
   * 
   * @param region
   *          region to open
   * @return RegionOpeningState 
   *         OPENED         - if region open request was successful.
   *         ALREADY_OPENED - if the region was already opened. 
   *         FAILED_OPENING - if region opening failed.
   *
   * @throws IOException
   */
  public RegionOpeningState openRegion(final HRegionInfo region) throws IOException;

  /**
   * Opens the specified region.
   * @param region
   *          region to open
   * @param versionOfOfflineNode
   *          the version of znode to compare when RS transitions the znode from
   *          OFFLINE state.
   * @return RegionOpeningState 
   *         OPENED         - if region open request was successful.
   *         ALREADY_OPENED - if the region was already opened. 
   *         FAILED_OPENING - if region opening failed.
   * @throws IOException
   */
  public RegionOpeningState openRegion(HRegionInfo region, int versionOfOfflineNode)
      throws IOException;
  
  /**
   * Opens the specified regions.
   * @param regions regions to open
   * @throws IOException
   */
  public void openRegions(final List<HRegionInfo> regions) throws IOException;

  /**
   * Closes the specified region.
   * @param region region to close
   * @return true if closing region, false if not
   * @throws IOException
   */
  public boolean closeRegion(final HRegionInfo region)
  throws IOException;

  /**
   * Closes the specified region and will use or not use ZK during the close
   * according to the specified flag.
   * @param region region to close
   * @param zk true if transitions should be done in ZK, false if not
   * @return true if closing region, false if not
   * @throws IOException
   */
  public boolean closeRegion(final HRegionInfo region, final boolean zk)
  throws IOException;
  
  /**
   * Closes the region in the RS with the specified encoded regionName and will
   * use or not use ZK during the close according to the specified flag. Note
   * that the encoded region name is in byte format.
   * 
   * @param encodedRegionName
   *          in bytes
   * @param zk
   *          true if to use zookeeper, false if need not.
   * @return true if region is closed, false if not.
   * @throws IOException
   */
  public boolean closeRegion(byte[] encodedRegionName, final boolean zk)
      throws IOException;

  // Region administrative methods

  /**
   * Flushes the MemStore of the specified region.
   * <p>
   * This method is synchronous.
   * @param regionInfo region to flush
   * @throws NotServingRegionException
   * @throws IOException
   */
  void flushRegion(HRegionInfo regionInfo)
  throws NotServingRegionException, IOException;

  /**
   * Splits the specified region.
   * <p>
   * This method currently flushes the region and then forces a compaction which
   * will then trigger a split.  The flush is done synchronously but the
   * compaction is asynchronous.
   * @param regionInfo region to split
   * @throws NotServingRegionException
   * @throws IOException
   */
  void splitRegion(HRegionInfo regionInfo)
  throws NotServingRegionException, IOException;

  /**
   * Splits the specified region.
   * <p>
   * This method currently flushes the region and then forces a compaction which
   * will then trigger a split.  The flush is done synchronously but the
   * compaction is asynchronous.
   * @param regionInfo region to split
   * @param splitPoint the explicit row to split on
   * @throws NotServingRegionException
   * @throws IOException
   */
  void splitRegion(HRegionInfo regionInfo, byte[] splitPoint)
  throws NotServingRegionException, IOException;

  /**
   * Compacts the specified region.  Performs a major compaction if specified.
   * <p>
   * This method is asynchronous.
   * @param regionInfo region to compact
   * @param major true to force major compaction
   * @throws NotServingRegionException
   * @throws IOException
   */
  void compactRegion(HRegionInfo regionInfo, boolean major)
  throws NotServingRegionException, IOException;

  /**
   * Replicates the given entries. The guarantee is that the given entries
   * will be durable on the slave cluster if this method returns without
   * any exception.
   * hbase.replication has to be set to true for this to work.
   *
   * @param entries entries to replicate
   * @throws IOException
   */
  public void replicateLogEntries(HLog.Entry[] entries) throws IOException;

  /**
   * Executes a single {@link org.apache.hadoop.hbase.ipc.CoprocessorProtocol}
   * method using the registered protocol handlers.
   * {@link CoprocessorProtocol} implementations must be registered via the
   * {@link org.apache.hadoop.hbase.regionserver.HRegion#registerProtocol(Class, org.apache.hadoop.hbase.ipc.CoprocessorProtocol)}
   * method before they are available.
   *
   * @param regionName name of the region against which the invocation is executed
   * @param call an {@code Exec} instance identifying the protocol, method name,
   *     and parameters for the method invocation
   * @return an {@code ExecResult} instance containing the region name of the
   *     invocation and the return value
   * @throws IOException if no registered protocol handler is found or an error
   *     occurs during the invocation
   * @see org.apache.hadoop.hbase.regionserver.HRegion#registerProtocol(Class, org.apache.hadoop.hbase.ipc.CoprocessorProtocol)
   */
  ExecResult execCoprocessor(byte[] regionName, Exec call)
      throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value match the expectedValue.
   * If it does, it adds the put. If passed expected value is null, then the
   * check is for non-existance of the row/column.
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param compareOp
   * @param comparator
   * @param put
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  public boolean checkAndPut(final byte[] regionName, final byte[] row,
      final byte[] family, final byte[] qualifier, final CompareOp compareOp,
      final WritableByteArrayComparable comparator, final Put put)
  throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value match the expectedValue.
   * If it does, it adds the delete. If passed expected value is null, then the
   * check is for non-existance of the row/column.
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param compareOp
   * @param comparator
   * @param delete
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  public boolean checkAndDelete(final byte[] regionName, final byte[] row,
     final byte[] family, final byte[] qualifier, final CompareOp compareOp,
     final WritableByteArrayComparable comparator, final Delete delete)
     throws IOException;
  
  /**
   * Performs a BlockCache summary and returns a List of BlockCacheColumnFamilySummary objects.
   * This method could be fairly heavyweight in that it evaluates the entire HBase file-system
   * against what is in the RegionServer BlockCache. 
   * 
   * @return BlockCacheColumnFamilySummary
   * @throws IOException exception
   */
  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries() throws IOException;
  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   * 
   * @throws IOException
   * @throws FailedLogCloseException
   * @return If lots of logs, flush the returned regions so next time through
   * we can clean logs. Returns null if nothing to flush.  Names are actual
   * region names as returned by {@link HRegionInfo#getEncodedName()} 
   */
  public byte[][] rollHLogWriter() throws IOException, FailedLogCloseException;
}
