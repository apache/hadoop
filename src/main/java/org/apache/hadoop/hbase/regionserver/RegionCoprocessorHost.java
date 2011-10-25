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

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
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
    public RegionEnvironment(final Coprocessor impl, final int priority,
        final int seq, final Configuration conf, final HRegion region,
        final RegionServerServices services) {
      super(impl, priority, seq, conf);
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

    // load system default cp's for user tables from configuration.
    if (!HTableDescriptor.isMetaTable(region.getRegionInfo().getTableName())) {
      loadSystemCoprocessors(conf, USER_REGION_COPROCESSOR_CONF_KEY);
    }

    // load Coprocessor From HDFS
    loadTableCoprocessors(conf);
  }

  void loadTableCoprocessors(final Configuration conf) {
    // scan the table attributes for coprocessor load specifications
    // initialize the coprocessors
    List<RegionEnvironment> configured = new ArrayList<RegionEnvironment>();
    for (Map.Entry<ImmutableBytesWritable,ImmutableBytesWritable> e:
        region.getTableDesc().getValues().entrySet()) {
      String key = Bytes.toString(e.getKey().get()).trim();
      String spec = Bytes.toString(e.getValue().get()).trim();
      if (HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(key).matches()) {
        // found one
        try {
          Matcher matcher = HConstants.CP_HTD_ATTR_VALUE_PATTERN.matcher(spec);
          if (matcher.matches()) {
            // jar file path can be empty if the cp class can be loaded
            // from class loader.
            Path path = matcher.group(1).trim().isEmpty() ?
                null : new Path(matcher.group(1).trim());
            String className = matcher.group(2).trim();
            int priority = matcher.group(3).trim().isEmpty() ?
                Coprocessor.PRIORITY_USER : Integer.valueOf(matcher.group(3));
            String cfgSpec = null;
            try {
              cfgSpec = matcher.group(4);
            } catch (IndexOutOfBoundsException ex) {
              // ignore
            }
            if (cfgSpec != null) {
              cfgSpec = cfgSpec.substring(cfgSpec.indexOf('|') + 1);
              Configuration newConf = HBaseConfiguration.create(conf);
              Matcher m = HConstants.CP_HTD_ATTR_VALUE_PARAM_PATTERN.matcher(cfgSpec);
              while (m.find()) {
                newConf.set(m.group(1), m.group(2));
              }
              configured.add(load(path, className, priority, newConf));
            } else {
              configured.add(load(path, className, priority, conf));
            }
            LOG.info("Load coprocessor " + className + " from HTD of " +
              Bytes.toString(region.getTableDesc().getName()) +
                " successfully.");
          } else {
            throw new RuntimeException("specification does not match pattern");
          }
        } catch (Exception ex) {
          LOG.warn("attribute '" + key +
            "' has invalid coprocessor specification '" + spec + "'");
          LOG.warn(StringUtils.stringifyException(ex));
        }
      }
    }
    // add together to coprocessor set for COW efficiency
    coprocessors.addAll(configured);
  }

  @Override
  public RegionEnvironment createEnvironment(Class<?> implClass,
      Coprocessor instance, int priority, int seq, Configuration conf) {
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
    return new RegionEnvironment(instance, priority, seq, conf, region,
        rsServices);
  }

  @Override
  protected void abortServer(final CoprocessorEnvironment env, final Throwable e) {
    abortServer("regionserver", rsServices, env, e);
  }

  /**
   * HBASE-4014 : This is used by coprocessor hooks which are not declared to throw exceptions.
   *
   * For example, {@link
   * org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost#preOpen()} and
   * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost#postOpen()} are such hooks.
   *
   * See also {@link org.apache.hadoop.hbase.master.MasterCoprocessorHost#handleCoprocessorThrowable()}
   * @param env The coprocessor that threw the exception.
   * @param e The exception that was thrown.
   */
  private void handleCoprocessorThrowableNoRethrow(
      final CoprocessorEnvironment env, final Throwable e) {
    try {
      handleCoprocessorThrowable(env,e);
    } catch (IOException ioe) {
      // We cannot throw exceptions from the caller hook, so ignore.
      LOG.warn("handleCoprocessorThrowable() threw an IOException while attempting to handle Throwable " + e
        + ". Ignoring.",e);
    }
  }

  /**
   * Invoked before a region open
   */
  public void preOpen() {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
         try {
          ((RegionObserver)env.getInstance()).preOpen(ctx);
         } catch (Throwable e) {
           handleCoprocessorThrowableNoRethrow(env, e);
         }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * Invoked after a region open
   */
  public void postOpen() {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postOpen(ctx);
        } catch (Throwable e) {
          handleCoprocessorThrowableNoRethrow(env, e);
        }
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).preClose(ctx, abortRequested);
        } catch (Throwable e) {
          handleCoprocessorThrowableNoRethrow(env, e);
        }
      }
    }
  }

  /**
   * Invoked after a region is closed
   * @param abortRequested true if the server is aborting
   */
  public void postClose(boolean abortRequested) {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postClose(ctx, abortRequested);
        } catch (Throwable e) {
          handleCoprocessorThrowableNoRethrow(env, e);
        }

      }
      shutdown(env);
    }
  }

  /**
   * Called prior to selecting the {@link StoreFile}s for compaction from
   * the list of currently available candidates.
   * @param store The store where compaction is being requested
   * @param candidates The currently available store files
   * @return If {@code true}, skip the normal selection process and use the current list
   */
  public boolean preCompactSelection(Store store, List<StoreFile> candidates) {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    boolean bypass = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        ((RegionObserver)env.getInstance()).preCompactSelection(
            ctx, store, candidates);
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  /**
   * Called after the {@link StoreFile}s to be compacted have been selected
   * from the available candidates.
   * @param store The store where compaction is being requested
   * @param selected The store files selected to compact
   */
  public void postCompactSelection(Store store,
      ImmutableList<StoreFile> selected) {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postCompactSelection(
              ctx, store, selected);
        } catch (Throwable e) {
          handleCoprocessorThrowableNoRethrow(env,e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * Called prior to rewriting the store files selected for compaction
   * @param store the store being compacted
   * @param scanner the scanner used to read store data during compaction
   */
  public InternalScanner preCompact(Store store, InternalScanner scanner) {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    boolean bypass = false;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          scanner = ((RegionObserver)env.getInstance()).preCompact(
              ctx, store, scanner);
        } catch (Throwable e) {
          handleCoprocessorThrowableNoRethrow(env,e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return bypass ? null : scanner;
  }

  /**
   * Called after the store compaction has completed.
   * @param store the store being compacted
   * @param resultFile the new store file written during compaction
   */
  public void postCompact(Store store, StoreFile resultFile) {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postCompact(ctx, store, resultFile);
        } catch (Throwable e) {
          handleCoprocessorThrowableNoRethrow(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * Invoked before a memstore flush
   */
  public void preFlush() {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).preFlush(ctx);
        } catch (Throwable e) {
          handleCoprocessorThrowableNoRethrow(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * Invoked after a memstore flush
   */
  public void postFlush() {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postFlush(ctx);
        } catch (Throwable e) {
          handleCoprocessorThrowableNoRethrow(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * Invoked just before a split
   */
  public void preSplit() {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).preSplit(ctx);
        } catch (Throwable e) {
          handleCoprocessorThrowableNoRethrow(env, e);
        }
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postSplit(ctx, l, r);
        } catch (Throwable e) {
          handleCoprocessorThrowableNoRethrow(env, e);
        }
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).preGetClosestRowBefore(ctx, row,
              family, result);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postGetClosestRowBefore(ctx, row,
              family, result);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).preGet(ctx, get, results);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  /**
   * @param get the Get request
   * @param results the result set
   * @exception IOException Exception
   */
  public void postGet(final Get get, final List<KeyValue> results)
  throws IOException {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postGet(ctx, get, results);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          exists = ((RegionObserver)env.getInstance()).preExists(ctx, get, exists);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          exists = ((RegionObserver)env.getInstance()).postExists(ctx, get, exists);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return exists;
  }

  /**
   * @param put The Put object
   * @param edit The WALEdit object.
   * @param writeToWAL true if the change should be written to the WAL
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean prePut(Put put, WALEdit edit,
      final boolean writeToWAL) throws IOException {
    boolean bypass = false;
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).prePut(ctx, put, edit, writeToWAL);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  /**
   * @param put The Put object
   * @param edit The WALEdit object.
   * @param writeToWAL true if the change should be written to the WAL
   * @exception IOException Exception
   */
  public void postPut(Put put, WALEdit edit,
      final boolean writeToWAL) throws IOException {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postPut(ctx, put, edit, writeToWAL);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  /**
   * @param delete The Delete object
   * @param edit The WALEdit object.
   * @param writeToWAL true if the change should be written to the WAL
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean preDelete(Delete delete, WALEdit edit,
      final boolean writeToWAL) throws IOException {
    boolean bypass = false;
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).preDelete(ctx, delete, edit, writeToWAL);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  /**
   * @param delete The Delete object
   * @param edit The WALEdit object.
   * @param writeToWAL true if the change should be written to the WAL
   * @exception IOException Exception
   */
  public void postDelete(Delete delete, WALEdit edit,
      final boolean writeToWAL) throws IOException {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postDelete(ctx, delete, edit, writeToWAL);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          result = ((RegionObserver)env.getInstance()).preCheckAndPut(ctx, row, family,
            qualifier, compareOp, comparator, put, result);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }


        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          result = ((RegionObserver)env.getInstance()).postCheckAndPut(ctx, row,
            family, qualifier, compareOp, comparator, put, result);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          result = ((RegionObserver)env.getInstance()).preCheckAndDelete(ctx, row,
            family, qualifier, compareOp, comparator, delete, result);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          result = ((RegionObserver)env.getInstance())
            .postCheckAndDelete(ctx, row, family, qualifier, compareOp,
              comparator, delete, result);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          amount = ((RegionObserver)env.getInstance()).preIncrementColumnValue(ctx,
              row, family, qualifier, amount, writeToWAL);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          result = ((RegionObserver)env.getInstance()).postIncrementColumnValue(ctx,
              row, family, qualifier, amount, writeToWAL, result);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).preIncrement(ctx, increment, result);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return bypass ? result : null;
  }

  /**
   * @param increment increment object
   * @param result the result returned by postIncrement
   * @throws IOException if an error occurred on the coprocessor
   */
  public void postIncrement(final Increment increment, Result result)
      throws IOException {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postIncrement(ctx, increment, result);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
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
  public RegionScanner preScannerOpen(Scan scan) throws IOException {
    boolean bypass = false;
    RegionScanner s = null;
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          s = ((RegionObserver)env.getInstance()).preScannerOpen(ctx, scan, s);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
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
  public RegionScanner postScannerOpen(final Scan scan, RegionScanner s)
      throws IOException {
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          s = ((RegionObserver)env.getInstance()).postScannerOpen(ctx, scan, s);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          hasNext = ((RegionObserver)env.getInstance()).preScannerNext(ctx, s, results,
            limit, hasNext);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          hasMore = ((RegionObserver)env.getInstance()).postScannerNext(ctx, s,
            results, limit, hasMore);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).preScannerClose(ctx, s);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postScannerClose(ctx, s);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).preWALRestore(ctx, info, logKey,
              logEdit);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
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
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionObserver)env.getInstance()).postWALRestore(ctx, info,
              logKey, logEdit);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }
}
