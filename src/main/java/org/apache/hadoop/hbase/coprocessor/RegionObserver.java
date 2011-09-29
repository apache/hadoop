/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * Coprocessors implement this interface to observe and mediate client actions
 * on the region.
 */
public interface RegionObserver extends Coprocessor {

  /**
   * Called before the region is reported as open to the master.
   * @param c the environment provided by the region server
   */
  void preOpen(final ObserverContext<RegionCoprocessorEnvironment> c);

  /**
   * Called after the region is reported as open to the master.
   * @param c the environment provided by the region server
   */
  void postOpen(final ObserverContext<RegionCoprocessorEnvironment> c);

  /**
   * Called before the memstore is flushed to disk.
   * @param c the environment provided by the region server
   */
  void preFlush(final ObserverContext<RegionCoprocessorEnvironment> c);

  /**
   * Called after the memstore is flushed to disk.
   * @param c the environment provided by the region server
   */
  void postFlush(final ObserverContext<RegionCoprocessorEnvironment> c);

  /**
   * Called prior to selecting the {@link StoreFile}s to compact from the list
   * of available candidates.  To alter the files used for compaction, you may
   * mutate the passed in list of candidates.
   * @param c the environment provided by the region server
   * @param store the store where compaction is being requested
   * @param candidates the store files currently available for compaction
   */
  void preCompactSelection(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Store store, final List<StoreFile> candidates);

  /**
   * Called after the {@link StoreFile}s to compact have been selected from the
   * available candidates.
   * @param c the environment provided by the region server
   * @param store the store being compacted
   * @param selected the store files selected to compact
   */
  void postCompactSelection(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Store store, final ImmutableList<StoreFile> selected);

  /**
   * Called prior to writing the {@link StoreFile}s selected for compaction into
   * a new {@code StoreFile}.  To override or modify the compaction process,
   * implementing classes have two options:
   * <ul>
   *   <li>Wrap the provided {@link InternalScanner} with a custom
   *   implementation that is returned from this method.  The custom scanner
   *   can then inspect {@link KeyValue}s from the wrapped scanner, applying
   *   its own policy to what gets written.</li>
   *   <li>Call {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()}
   *   and provide a custom implementation for writing of new
   *   {@link StoreFile}s.  <strong>Note: any implementations bypassing
   *   core compaction using this approach must write out new store files
   *   themselves or the existing data will no longer be available after
   *   compaction.</strong></li>
   * </ul>
   * @param c the environment provided by the region server
   * @param store the store being compacted
   * @param scanner the scanner over existing data used in the store file
   * rewriting
   * @return the scanner to use during compaction.  Should not be {@code null}
   * unless the implementation is writing new store files on its own.
   */
  InternalScanner preCompact(final ObserverContext<RegionCoprocessorEnvironment> c,
    final Store store, final InternalScanner scanner);

  /**
   * Called after compaction has completed and the new store file has been
   * moved in to place.
   * @param c the environment provided by the region server
   * @param store the store being compacted
   * @param resultFile the new store file written out during compaction
   */
  void postCompact(final ObserverContext<RegionCoprocessorEnvironment> c,
    final Store store, StoreFile resultFile);

  /**
   * Called before the region is split.
   * @param c the environment provided by the region server
   * (e.getRegion() returns the parent region)
   */
  void preSplit(final ObserverContext<RegionCoprocessorEnvironment> c);

  /**
   * Called after the region is split.
   * @param c the environment provided by the region server
   * (e.getRegion() returns the parent region)
   * @param l the left daughter region
   * @param r the right daughter region
   */
  void postSplit(final ObserverContext<RegionCoprocessorEnvironment> c, final HRegion l,
    final HRegion r);

  /**
   * Called before the region is reported as closed to the master.
   * @param c the environment provided by the region server
   * @param abortRequested true if the region server is aborting
   */
  void preClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      boolean abortRequested);

  /**
   * Called after the region is reported as closed to the master.
   * @param c the environment provided by the region server
   * @param abortRequested true if the region server is aborting
   */
  void postClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      boolean abortRequested);

  /**
   * Called before a client makes a GetClosestRowBefore request.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param row the row
   * @param family the family
   * @param result The result to return to the client if default processing
   * is bypassed. Can be modified. Will not be used if default processing
   * is not bypassed.
   * @throws IOException if an error occurred on the coprocessor
   */
  void preGetClosestRowBefore(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final Result result)
    throws IOException;

  /**
   * Called after a client makes a GetClosestRowBefore request.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param row the row
   * @param family the desired family
   * @param result the result to return to the client, modify as necessary
   * @throws IOException if an error occurred on the coprocessor
   */
  void postGetClosestRowBefore(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final Result result)
    throws IOException;

  /**
   * Called before the client performs a Get
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param get the Get request
   * @param result The result to return to the client if default processing
   * is bypassed. Can be modified. Will not be used if default processing
   * is not bypassed.
   * @throws IOException if an error occurred on the coprocessor
   */
  void preGet(final ObserverContext<RegionCoprocessorEnvironment> c, final Get get,
      final List<KeyValue> result)
    throws IOException;

  /**
   * Called after the client performs a Get
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param get the Get request
   * @param result the result to return to the client, modify as necessary
   * @throws IOException if an error occurred on the coprocessor
   */
  void postGet(final ObserverContext<RegionCoprocessorEnvironment> c, final Get get,
      final List<KeyValue> result)
    throws IOException;

  /**
   * Called before the client tests for existence using a Get.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param get the Get request
   * @param exists
   * @return the value to return to the client if bypassing default processing
   * @throws IOException if an error occurred on the coprocessor
   */
  boolean preExists(final ObserverContext<RegionCoprocessorEnvironment> c, final Get get,
      final boolean exists)
    throws IOException;

  /**
   * Called after the client tests for existence using a Get.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param get the Get request
   * @param exists the result returned by the region server
   * @return the result to return to the client
   * @throws IOException if an error occurred on the coprocessor
   */
  boolean postExists(final ObserverContext<RegionCoprocessorEnvironment> c, final Get get,
      final boolean exists)
    throws IOException;

  /**
   * Called before the client stores a value.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param put The Put object
   * @param edit The WALEdit object that will be written to the wal
   * @param writeToWAL true if the change should be written to the WAL
   * @throws IOException if an error occurred on the coprocessor
   */
  void prePut(final ObserverContext<RegionCoprocessorEnvironment> c, 
      final Put put, final WALEdit edit, final boolean writeToWAL)
    throws IOException;

  /**
   * Called after the client stores a value.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param put The Put object
   * @param edit The WALEdit object for the wal
   * @param writeToWAL true if the change should be written to the WAL
   * @throws IOException if an error occurred on the coprocessor
   */
  void postPut(final ObserverContext<RegionCoprocessorEnvironment> c, 
      final Put put, final WALEdit edit, final boolean writeToWAL)
    throws IOException;

  /**
   * Called before the client deletes a value.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param delete The Delete object
   * @param edit The WALEdit object for the wal
   * @param writeToWAL true if the change should be written to the WAL
   * @throws IOException if an error occurred on the coprocessor
   */
  void preDelete(final ObserverContext<RegionCoprocessorEnvironment> c, 
      final Delete delete, final WALEdit edit, final boolean writeToWAL)
    throws IOException;

  /**
   * Called after the client deletes a value.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param delete The Delete object
   * @param edit The WALEdit object for the wal
   * @param writeToWAL true if the change should be written to the WAL
   * @throws IOException if an error occurred on the coprocessor
   */
  void postDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Delete delete, final WALEdit edit, final boolean writeToWAL)
    throws IOException;

  /**
   * Called before checkAndPut
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param put data to put if check succeeds
   * @param result 
   * @return the return value to return to client if bypassing default
   * processing
   * @throws IOException if an error occurred on the coprocessor
   */
  boolean preCheckAndPut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final WritableByteArrayComparable comparator,
      final Put put, final boolean result)
    throws IOException;

  /**
   * Called after checkAndPut
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param put data to put if check succeeds
   * @param result from the checkAndPut
   * @return the possibly transformed return value to return to client
   * @throws IOException if an error occurred on the coprocessor
   */
  boolean postCheckAndPut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final WritableByteArrayComparable comparator,
      final Put put, final boolean result)
    throws IOException;

  /**
   * Called before checkAndDelete
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param delete delete to commit if check succeeds
   * @param result 
   * @return the value to return to client if bypassing default processing
   * @throws IOException if an error occurred on the coprocessor
   */
  boolean preCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final WritableByteArrayComparable comparator,
      final Delete delete, final boolean result)
    throws IOException;

  /**
   * Called after checkAndDelete
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param delete delete to commit if check succeeds
   * @param result from the CheckAndDelete
   * @return the possibly transformed returned value to return to client
   * @throws IOException if an error occurred on the coprocessor
   */
  boolean postCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final WritableByteArrayComparable comparator,
      final Delete delete, final boolean result)
    throws IOException;

  /**
   * Called before incrementColumnValue
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL true if the change should be written to the WAL
   * @return value to return to the client if bypassing default processing
   * @throws IOException if an error occurred on the coprocessor
   */
  long preIncrementColumnValue(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL)
    throws IOException;

  /**
   * Called after incrementColumnValue
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL true if the change should be written to the WAL
   * @param result the result returned by incrementColumnValue
   * @return the result to return to the client
   * @throws IOException if an error occurred on the coprocessor
   */
  long postIncrementColumnValue(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL, final long result)
    throws IOException;

  /**
   * Called before Increment
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param increment increment object
   * @param result The result to return to the client if default processing
   * is bypassed. Can be modified. Will not be used if default processing
   * is not bypassed.
   * @throws IOException if an error occurred on the coprocessor
   */
  void preIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment, final Result result)
    throws IOException;

  /**
   * Called after increment
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param increment increment object
   * @param result the result returned by increment, can be modified
   * @throws IOException if an error occurred on the coprocessor
   */
  void postIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment, final Result result)
    throws IOException;

  /**
   * Called before the client opens a new scanner.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param scan the Scan specification
   * @param s if not null, the base scanner
   * @return an RegionScanner instance to use instead of the base scanner if
   * overriding default behavior, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  RegionScanner preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan, final RegionScanner s)
    throws IOException;

  /**
   * Called after the client opens a new scanner.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param scan the Scan specification
   * @param s if not null, the base scanner
   * @return the scanner instance to use
   * @throws IOException if an error occurred on the coprocessor
   */
  RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan, final RegionScanner s)
    throws IOException;

  /**
   * Called before the client asks for the next row on a scanner.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param s the scanner
   * @param result The result to return to the client if default processing
   * is bypassed. Can be modified. Will not be returned if default processing
   * is not bypassed.
   * @param limit the maximum number of results to return
   * @param hasNext the 'has more' indication
   * @return 'has more' indication that should be sent to client
   * @throws IOException if an error occurred on the coprocessor
   */
  boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s, final List<Result> result,
      final int limit, final boolean hasNext)
    throws IOException;

  /**
   * Called after the client asks for the next row on a scanner.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param s the scanner
   * @param result the result to return to the client, can be modified
   * @param limit the maximum number of results to return
   * @param hasNext the 'has more' indication
   * @return 'has more' indication that should be sent to client
   * @throws IOException if an error occurred on the coprocessor
   */
  boolean postScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s, final List<Result> result, final int limit,
      final boolean hasNext)
    throws IOException;

  /**
   * Called before the client closes a scanner.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param s the scanner
   * @throws IOException if an error occurred on the coprocessor
   */
  void preScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s)
    throws IOException;

  /**
   * Called after the client closes a scanner.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param s the scanner
   * @throws IOException if an error occurred on the coprocessor
   */
  void postScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s)
    throws IOException;

  /**
   * Called before a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * replayed for this region.
   *
   * @param ctx
   * @param info
   * @param logKey
   * @param logEdit
   * @throws IOException
   */
  void preWALRestore(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException;

  /**
   * Called after a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * replayed for this region.
   *
   * @param ctx
   * @param info
   * @param logKey
   * @param logEdit
   * @throws IOException
   */
  void postWALRestore(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException;
}
