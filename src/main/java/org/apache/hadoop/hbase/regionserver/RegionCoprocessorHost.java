/*
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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implements the coprocessor environment and runtime support for coprocessors
 * loaded within a {@link HRegion}.
 */
public class RegionCoprocessorHost
    extends CoprocessorHost<RegionCoprocessorHost.RegionEnvironment> {

  private static final Log LOG = LogFactory.getLog(RegionCoprocessorHost.class);

  /**
   * Encapsulation of the environment of each coprocessor
   */
  static class RegionEnvironment extends CoprocessorHost.Environment
      implements RegionCoprocessorEnvironment {

    private HRegion region;
    private RegionServerServices rsServices;

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     */
    public RegionEnvironment(final Coprocessor impl,
        final Coprocessor.Priority priority, final int seq, final HRegion region,
        final RegionServerServices services) {
      super(impl, priority, seq);
      this.region = region;
      this.rsServices = services;
    }

    /** @return the region */
    @Override
    public HRegion getRegion() {
      return region;
    }

    /** @return reference to the region server services */
    @Override
    public RegionServerServices getRegionServerServices() {
      return rsServices;
    }

    public void shutdown() {
      super.shutdown();
    }
  }

  static final Pattern attrSpecMatch = Pattern.compile("(.+):(.+):(.+)");

  /** The region server services */
  RegionServerServices rsServices;
  /** The region */
  HRegion region;

  /**
   * Constructor
   * @param region the region
   * @param rsServices interface to available region server functionality
   * @param conf the configuration
   */
  public RegionCoprocessorHost(final HRegion region,
      final RegionServerServices rsServices, final Configuration conf) {
    this.rsServices = rsServices;
    this.region = region;
    this.pathPrefix = this.region.getRegionNameAsString().replace(',', '_');

    // load system default cp's from configuration.
    loadSystemCoprocessors(conf, REGION_COPROCESSOR_CONF_KEY);

    // load Coprocessor From HDFS
    loadTableCoprocessors();
  }

  void loadTableCoprocessors () {
    // scan the table attributes for coprocessor load specifications
    // initialize the coprocessors
    List<RegionEnvironment> configured = new ArrayList<RegionEnvironment>();
    for (Map.Entry<ImmutableBytesWritable,ImmutableBytesWritable> e:
        region.getTableDesc().getValues().entrySet()) {
      String key = Bytes.toString(e.getKey().get());
      if (key.startsWith("COPROCESSOR")) {
        // found one
        try {
          String spec = Bytes.toString(e.getValue().get());
          Matcher matcher = attrSpecMatch.matcher(spec);
          if (matcher.matches()) {
            Path path = new Path(matcher.group(1));
            String className = matcher.group(2);
            Coprocessor.Priority priority =
              Coprocessor.Priority.valueOf(matcher.group(3));
            configured.add(load(path, className, priority));
            LOG.info("Load coprocessor " + className + " from HTD of " +
                Bytes.toString(region.getTableDesc().getName()) +
                " successfully.");
          } else {
            LOG.warn("attribute '" + key + "' has invalid coprocessor spec");
          }
        } catch (IOException ex) {
            LOG.warn(StringUtils.stringifyException(ex));
        }
      }
    }
    // add together to coprocessor set for COW efficiency
    coprocessors.addAll(configured);
  }

  @Override
  public RegionEnvironment createEnvironment(
      Class<?> implClass, Coprocessor instance, Coprocessor.Priority priority, int seq) {
    // Check if it's an Endpoint.
    // Due to current dynamic protocol design, Endpoint
    // uses a different way to be registered and executed.
    // It uses a visitor pattern to invoke registered Endpoint
    // method.
    for (Class c : implClass.getInterfaces()) {
      if (CoprocessorProtocol.class.isAssignableFrom(c)) {
        region.registerProtocol(c, (CoprocessorProtocol)instance);
        break;
      }
    }

    return new RegionEnvironment(instance, priority, seq, region, rsServices);
  }

  /**
   * Invoked before a region open
   */
  public void preOpen() {
    loadTableCoprocessors();
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).preOpen(env);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * Invoked after a region open
   */
  public void postOpen() {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).postOpen(env);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * Invoked before a region is closed
   * @param abortRequested true if the server is aborting
   */
  public void preClose(boolean abortRequested) {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).preClose(env, abortRequested);
      }
    }
  }

  /**
   * Invoked after a region is closed
   * @param abortRequested true if the server is aborting
   */
  public void postClose(boolean abortRequested) {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).postClose(env, abortRequested);
      }
      shutdown(env);
    }
  }

  /**
   * Invoked before a region is compacted.
   * @param willSplit true if the compaction is about to trigger a split
   */
  public void preCompact(boolean willSplit) {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).preCompact(env, willSplit);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * Invoked after a region is compacted.
   * @param willSplit true if the compaction is about to trigger a split
   */
  public void postCompact(boolean willSplit) {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).postCompact(env, willSplit);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * Invoked before a memstore flush
   */
  public void preFlush() {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).preFlush(env);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * Invoked after a memstore flush
   */
  public void postFlush() {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).postFlush(env);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * Invoked just before a split
   */
  public void preSplit() {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).preSplit(env);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * Invoked just after a split
   * @param l the new left-hand daughter region
   * @param r the new right-hand daughter region
   */
  public void postSplit(HRegion l, HRegion r) {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).postSplit(env, l, r);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  // RegionObserver support

  /**
   * @param row the row key
   * @param family the family
   * @param result the result set from the region
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean preGetClosestRowBefore(final byte[] row, final byte[] family,
      final Result result) throws IOException {
    boolean bypass = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).preGetClosestRowBefore(env, row, family,
          result);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  /**
   * @param row the row key
   * @param family the family
   * @param result the result set from the region
   * @exception IOException Exception
   */
  public void postGetClosestRowBefore(final byte[] row, final byte[] family,
      final Result result) throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).postGetClosestRowBefore(env, row, family,
          result);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * @param get the Get request
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean preGet(final Get get, final List<KeyValue> results)
      throws IOException {
    boolean bypass = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).preGet(env, get, results);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  /**
   * @param get the Get request
   * @param results the result set
   * @return the possibly transformed result set to use
   * @exception IOException Exception
   */
  public void postGet(final Get get, final List<KeyValue> results)
  throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).postGet(env, get, results);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * @param get the Get request
   * @return true or false to return to client if bypassing normal operation,
   * or null otherwise
   * @exception IOException Exception
   */
  public Boolean preExists(final Get get) throws IOException {
    boolean bypass = false;
    boolean exists = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        exists = ((RegionObserver)env.getInstance()).preExists(env, get, exists);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass ? exists : null;
  }

  /**
   * @param get the Get request
   * @param exists the result returned by the region server
   * @return the result to return to the client
   * @exception IOException Exception
   */
  public boolean postExists(final Get get, boolean exists)
      throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        exists = ((RegionObserver)env.getInstance()).postExists(env, get, exists);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return exists;
  }

  /**
   * @param familyMap map of family to edits for the given family.
   * @param writeToWAL true if the change should be written to the WAL
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean prePut(final Map<byte[], List<KeyValue>> familyMap,
      final boolean writeToWAL) throws IOException {
    boolean bypass = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).prePut(env, familyMap, writeToWAL);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  /**
   * @param familyMap map of family to edits for the given family.
   * @param writeToWAL true if the change should be written to the WAL
   * @exception IOException Exception
   */
  public void postPut(final Map<byte[], List<KeyValue>> familyMap,
      final boolean writeToWAL) throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).postPut(env, familyMap, writeToWAL);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * @param familyMap map of family to edits for the given family.
   * @param writeToWAL true if the change should be written to the WAL
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean preDelete(final Map<byte[], List<KeyValue>> familyMap,
      final boolean writeToWAL) throws IOException {
    boolean bypass = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).preDelete(env, familyMap, writeToWAL);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  /**
   * @param familyMap map of family to edits for the given family.
   * @param writeToWAL true if the change should be written to the WAL
   * @exception IOException Exception
   */
  public void postDelete(final Map<byte[], List<KeyValue>> familyMap,
      final boolean writeToWAL) throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).postDelete(env, familyMap, writeToWAL);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param put data to put if check succeeds
   * @return true or false to return to client if default processing should
   * be bypassed, or null otherwise
   * @throws IOException e
   */
  public Boolean preCheckAndPut(final byte [] row, final byte [] family,
      final byte [] qualifier, final CompareOp compareOp,
      final WritableByteArrayComparable comparator, Put put)
    throws IOException {
    boolean bypass = false;
    boolean result = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        result = ((RegionObserver)env.getInstance()).preCheckAndPut(env, row, family,
          qualifier, compareOp, comparator, put, result);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass ? result : null;
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param put data to put if check succeeds
   * @throws IOException e
   */
  public boolean postCheckAndPut(final byte [] row, final byte [] family,
      final byte [] qualifier, final CompareOp compareOp,
      final WritableByteArrayComparable comparator, final Put put,
      boolean result)
    throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        result = ((RegionObserver)env.getInstance()).postCheckAndPut(env, row,
          family, qualifier, compareOp, comparator, put, result);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return result;
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param delete delete to commit if check succeeds
   * @return true or false to return to client if default processing should
   * be bypassed, or null otherwise
   * @throws IOException e
   */
  public Boolean preCheckAndDelete(final byte [] row, final byte [] family,
      final byte [] qualifier, final CompareOp compareOp,
      final WritableByteArrayComparable comparator, Delete delete)
      throws IOException {
    boolean bypass = false;
    boolean result = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        result = ((RegionObserver)env.getInstance()).preCheckAndDelete(env, row,
          family, qualifier, compareOp, comparator, delete, result);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass ? result : null;
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param delete delete to commit if check succeeds
   * @throws IOException e
   */
  public boolean postCheckAndDelete(final byte [] row, final byte [] family,
      final byte [] qualifier, final CompareOp compareOp,
      final WritableByteArrayComparable comparator, final Delete delete,
      boolean result)
    throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        result = ((RegionObserver)env.getInstance())
          .postCheckAndDelete(env, row, family, qualifier, compareOp,
            comparator, delete, result);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return result;
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL true if the change should be written to the WAL
   * @return return value for client if default operation should be bypassed,
   * or null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Long preIncrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, long amount, final boolean writeToWAL)
      throws IOException {
    boolean bypass = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        amount = ((RegionObserver)env.getInstance()).preIncrementColumnValue(env,
            row, family, qualifier, amount, writeToWAL);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass ? amount : null;
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL true if the change should be written to the WAL
   * @param result the result returned by incrementColumnValue
   * @return the result to return to the client
   * @throws IOException if an error occurred on the coprocessor
   */
  public long postIncrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final boolean writeToWAL,
      long result) throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        result = ((RegionObserver)env.getInstance()).postIncrementColumnValue(env,
            row, family, qualifier, amount, writeToWAL, result);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return result;
  }

  /**
   * @param increment increment object
   * @return result to return to client if default operation should be
   * bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preIncrement(Increment increment)
      throws IOException {
    boolean bypass = false;
    Result result = new Result();
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).preIncrement(env, increment, result);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass ? result : null;
  }

  /**
   * @param increment increment object
   * @param result the result returned by incrementColumnValue
   * @throws IOException if an error occurred on the coprocessor
   */
  public void postIncrement(final Increment increment, Result result)
      throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).postIncrement(env, increment, result);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * @param scan the Scan specification
   * @return scanner id to return to client if default operation should be
   * bypassed, false otherwise
   * @exception IOException Exception
   */
  public InternalScanner preScannerOpen(Scan scan) throws IOException {
    boolean bypass = false;
    InternalScanner s = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        s = ((RegionObserver)env.getInstance()).preScannerOpen(env, scan, s);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass ? s : null;
  }

  /**
   * @param scan the Scan specification
   * @param s the scanner
   * @return the scanner instance to use
   * @exception IOException Exception
   */
  public InternalScanner postScannerOpen(final Scan scan, InternalScanner s)
      throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        s = ((RegionObserver)env.getInstance()).postScannerOpen(env, scan, s);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return s;
  }

  /**
   * @param s the scanner
   * @param results the result set returned by the region server
   * @param limit the maximum number of results to return
   * @return 'has next' indication to client if bypassing default behavior, or
   * null otherwise
   * @exception IOException Exception
   */
  public Boolean preScannerNext(final InternalScanner s,
      final List<Result> results, int limit) throws IOException {
    boolean bypass = false;
    boolean hasNext = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        hasNext = ((RegionObserver)env.getInstance()).preScannerNext(env, s, results,
          limit, hasNext);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass ? hasNext : null;
  }

  /**
   * @param s the scanner
   * @param results the result set returned by the region server
   * @param limit the maximum number of results to return
   * @param hasMore
   * @return 'has more' indication to give to client
   * @exception IOException Exception
   */
  public boolean postScannerNext(final InternalScanner s,
      final List<Result> results, final int limit, boolean hasMore)
      throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        hasMore = ((RegionObserver)env.getInstance()).postScannerNext(env, s,
          results, limit, hasMore);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return hasMore;
  }

  /**
   * @param s the scanner
   * @return true if default behavior should be bypassed, false otherwise
   * @exception IOException Exception
   */
  public boolean preScannerClose(final InternalScanner s)
      throws IOException {
    boolean bypass = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).preScannerClose(env, s);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  /**
   * @param s the scanner
   * @exception IOException Exception
   */
  public void postScannerClose(final InternalScanner s)
      throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).postScannerClose(env, s);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * @param info
   * @param logKey
   * @param logEdit
   * @return true if default behavior should be bypassed, false otherwise
   * @throws IOException
   */
  public boolean preWALRestore(HRegionInfo info, HLogKey logKey,
      WALEdit logEdit) throws IOException {
    boolean bypass = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).preWALRestore(env, info, logKey,
            logEdit);
      }
      bypass |= env.shouldBypass();
      if (env.shouldComplete()) {
        break;
      }
    }
    return bypass;
  }

  /**
   * @param info
   * @param logKey
   * @param logEdit
   * @throws IOException
   */
  public void postWALRestore(HRegionInfo info, HLogKey logKey,
      WALEdit logEdit) throws IOException {
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ((RegionObserver)env.getInstance()).postWALRestore(env, info,
            logKey, logEdit);
      }
      if (env.shouldComplete()) {
        break;
      }
    }
  }
}
