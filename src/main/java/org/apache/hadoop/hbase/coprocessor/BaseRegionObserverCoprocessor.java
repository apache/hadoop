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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;

/**
 * An abstract class that implements Coprocessor and RegionObserver.
 * By extending it, you can create you own region observer without
 * overriding all abstract methods of Coprocessor and RegionObserver.
 */
public abstract class BaseRegionObserverCoprocessor implements Coprocessor,
    RegionObserver {
  @Override
  public void start(CoprocessorEnvironment e) { }

  @Override
  public void stop(CoprocessorEnvironment e) { }

  @Override
  public void preOpen(CoprocessorEnvironment e) { }

  @Override
  public void postOpen(CoprocessorEnvironment e) { }

  @Override
  public void preClose(CoprocessorEnvironment e, boolean abortRequested)
    { }

  @Override
  public void postClose(CoprocessorEnvironment e, boolean abortRequested)
    { }

  @Override
  public void preFlush(CoprocessorEnvironment e) { }

  @Override
  public void postFlush(CoprocessorEnvironment e) { }

  @Override
  public void preSplit(CoprocessorEnvironment e) { }

  @Override
  public void postSplit(CoprocessorEnvironment e, HRegion l, HRegion r) { }

  @Override
  public void preCompact(CoprocessorEnvironment e, boolean willSplit) { }

  @Override
  public void postCompact(CoprocessorEnvironment e, boolean willSplit) { }

  @Override
  public void preGetClosestRowBefore(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final Result result)
    throws IOException {
  }

  @Override
  public void postGetClosestRowBefore(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final Result result)
      throws IOException {
  }

  @Override
  public void preGet(final CoprocessorEnvironment e, final Get get,
      final List<KeyValue> results) throws IOException {
  }

  @Override
  public void postGet(final CoprocessorEnvironment e, final Get get,
      final List<KeyValue> results) throws IOException {
  }

  @Override
  public boolean preExists(final CoprocessorEnvironment e, final Get get,
      final boolean exists) throws IOException {
    return exists;
  }

  @Override
  public boolean postExists(final CoprocessorEnvironment e, final Get get,
      boolean exists) throws IOException {
    return exists;
  }

  @Override
  public void prePut(final CoprocessorEnvironment e, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL) throws IOException {
  }

  @Override
  public void postPut(final CoprocessorEnvironment e, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL) throws IOException {
  }

  @Override
  public void preDelete(final CoprocessorEnvironment e, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL) throws IOException {
  }

  @Override
  public void postDelete(final CoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
      throws IOException {
  }

  @Override
  public boolean preCheckAndPut(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Put put, final boolean result)
      throws IOException {
    return result;
  }

  @Override
  public boolean postCheckAndPut(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Put put, final boolean result)
    throws IOException {
    return result;
  }

  @Override
  public boolean preCheckAndDelete(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Delete delete, final boolean result)
      throws IOException {
    return result;
  }

  @Override
  public boolean postCheckAndDelete(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Delete delete, final boolean result)
      throws IOException {
    return result;
  }

  @Override
  public long preIncrementColumnValue(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL) throws IOException {
    return amount;
  }

  @Override
  public long postIncrementColumnValue(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL, long result)
      throws IOException {
    return result;
  }

  @Override
  public void preIncrement(final CoprocessorEnvironment e,
      final Increment increment, final Result result) throws IOException {
  }

  @Override
  public void postIncrement(final CoprocessorEnvironment e,
      final Increment increment, final Result result) throws IOException {
  }

  @Override
  public InternalScanner preScannerOpen(final CoprocessorEnvironment e,
      final Scan scan, final InternalScanner s) throws IOException {
    return s;
  }

  @Override
  public InternalScanner postScannerOpen(final CoprocessorEnvironment e,
      final Scan scan, final InternalScanner s) throws IOException {
    return s;
  }

  @Override
  public boolean preScannerNext(final CoprocessorEnvironment e,
      final InternalScanner s, final List<KeyValue> results,
      final int limit, final boolean hasMore) throws IOException {
    return hasMore;
  }

  @Override
  public boolean postScannerNext(final CoprocessorEnvironment e,
      final InternalScanner s, final List<KeyValue> results, final int limit,
      final boolean hasMore) throws IOException {
    return hasMore;
  }

  @Override
  public void preScannerClose(final CoprocessorEnvironment e,
      final InternalScanner s) throws IOException {
  }

  @Override
  public void postScannerClose(final CoprocessorEnvironment e,
      final InternalScanner s) throws IOException {
  }
}
