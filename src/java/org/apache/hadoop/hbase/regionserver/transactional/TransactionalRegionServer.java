/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver.transactional;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.util.Progressable;

/**
 * RegionServer with support for transactions. Transactional logic is at the
 * region level, so we mostly just delegate to the appropriate
 * TransactionalRegion.
 */
public class TransactionalRegionServer extends HRegionServer implements
    TransactionalRegionInterface {
  static final Log LOG = LogFactory.getLog(TransactionalRegionServer.class);

  private final CleanOldTransactionsChore cleanOldTransactionsThread;

  /**
   * @param conf
   * @throws IOException
   */
  public TransactionalRegionServer(final HBaseConfiguration conf)
      throws IOException {
    this(new HServerAddress(conf.get(REGIONSERVER_ADDRESS,
        DEFAULT_REGIONSERVER_ADDRESS)), conf);
  }

  /**
   * @param address
   * @param conf
   * @throws IOException
   */
  public TransactionalRegionServer(final HServerAddress address,
      final HBaseConfiguration conf) throws IOException {
    super(address, conf);
    cleanOldTransactionsThread = new CleanOldTransactionsChore(this,
        super.stopRequested);
  }

  @Override
  public long getProtocolVersion(final String protocol, final long clientVersion)
      throws IOException {
    if (protocol.equals(TransactionalRegionInterface.class.getName())) {
      return HBaseRPCProtocolVersion.versionID;
    }
    return super.getProtocolVersion(protocol, clientVersion);
  }

  @Override
  protected void init(final MapWritable c) throws IOException {
    super.init(c);
    String n = Thread.currentThread().getName();
    UncaughtExceptionHandler handler = new UncaughtExceptionHandler() {
      public void uncaughtException(final Thread t, final Throwable e) {
        abort();
        LOG.fatal("Set stop flag in " + t.getName(), e);
      }
    };
    Threads.setDaemonThreadRunning(this.cleanOldTransactionsThread, n
        + ".oldTransactionCleaner", handler);

  }

  @Override
  protected HRegion instantiateRegion(final HRegionInfo regionInfo)
      throws IOException {
    HRegion r = new TransactionalRegion(HTableDescriptor.getTableDir(super
        .getRootDir(), regionInfo.getTableDesc().getName()), super.hlog, super
        .getFileSystem(), super.conf, regionInfo, super.getFlushRequester());
    r.initialize(null, new Progressable() {
      public void progress() {
        addProcessingMessage(regionInfo);
      }
    });
    return r;
  }

  protected TransactionalRegion getTransactionalRegion(final byte[] regionName)
      throws NotServingRegionException {
    return (TransactionalRegion) super.getRegion(regionName);
  }

  public void abort(final byte[] regionName, final long transactionId)
      throws IOException {
    checkOpen();
    super.getRequestCount().incrementAndGet();
    try {
      getTransactionalRegion(regionName).abort(transactionId);
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  public void batchUpdate(final long transactionId, final byte[] regionName,
      final BatchUpdate b) throws IOException {
    checkOpen();
    super.getRequestCount().incrementAndGet();
    try {
      getTransactionalRegion(regionName).batchUpdate(transactionId, b);
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  public void commit(final byte[] regionName, final long transactionId)
      throws IOException {
    checkOpen();
    super.getRequestCount().incrementAndGet();
    try {
      getTransactionalRegion(regionName).commit(transactionId);
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  public boolean commitRequest(final byte[] regionName, final long transactionId)
      throws IOException {
    checkOpen();
    super.getRequestCount().incrementAndGet();
    try {
      return getTransactionalRegion(regionName).commitRequest(transactionId);
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  public Cell get(final long transactionId, final byte[] regionName,
      final byte[] row, final byte[] column) throws IOException {
    checkOpen();
    super.getRequestCount().incrementAndGet();
    try {
      return getTransactionalRegion(regionName).get(transactionId, row, column);
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  public Cell[] get(final long transactionId, final byte[] regionName,
      final byte[] row, final byte[] column, final int numVersions)
      throws IOException {
    checkOpen();
    super.getRequestCount().incrementAndGet();
    try {
      return getTransactionalRegion(regionName).get(transactionId, row, column,
          numVersions);
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  public Cell[] get(final long transactionId, final byte[] regionName,
      final byte[] row, final byte[] column, final long timestamp,
      final int numVersions) throws IOException {
    checkOpen();
    super.getRequestCount().incrementAndGet();
    try {
      return getTransactionalRegion(regionName).get(transactionId, row, column,
          timestamp, numVersions);
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  public RowResult getRow(final long transactionId, final byte[] regionName,
      final byte[] row, final long ts) throws IOException {
    return getRow(transactionId, regionName, row, null, ts);
  }

  public RowResult getRow(final long transactionId, final byte[] regionName,
      final byte[] row, final byte[][] columns) throws IOException {
    return getRow(transactionId, regionName, row, columns,
        HConstants.LATEST_TIMESTAMP);
  }

  public RowResult getRow(final long transactionId, final byte[] regionName,
      final byte[] row, final byte[][] columns, final long ts)
      throws IOException {
    checkOpen();
    super.getRequestCount().incrementAndGet();
    try {
      // convert the columns array into a set so it's easy to check later.
      NavigableSet<byte[]> columnSet = null;
      if (columns != null) {
        columnSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        columnSet.addAll(Arrays.asList(columns));
      }

      TransactionalRegion region = getTransactionalRegion(regionName);
      Map<byte[], Cell> map = region.getFull(transactionId, row, columnSet, ts);
      HbaseMapWritable<byte[], Cell> result = new HbaseMapWritable<byte[], Cell>();
      result.putAll(map);
      return new RowResult(row, result);
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }

  }

  public void deleteAll(final long transactionId, final byte[] regionName,
      final byte[] row, final long timestamp) throws IOException {
    checkOpen();
    super.getRequestCount().incrementAndGet();
    try {
      TransactionalRegion region = getTransactionalRegion(regionName);
      region.deleteAll(transactionId, row, timestamp);
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  public long openScanner(final long transactionId, final byte[] regionName,
      final byte[][] cols, final byte[] firstRow, final long timestamp,
      final RowFilterInterface filter) throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if (regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if (cols == null) {
      npe = new NullPointerException("columns to scan is null");
    } else if (firstRow == null) {
      npe = new NullPointerException("firstRow for scanner is null");
    }
    if (npe != null) {
      IOException io = new IOException("Invalid arguments to openScanner");
      io.initCause(npe);
      throw io;
    }
    super.getRequestCount().incrementAndGet();
    try {
      TransactionalRegion r = getTransactionalRegion(regionName);
      long scannerId = -1L;
      InternalScanner s = r.getScanner(transactionId, cols, firstRow,
          timestamp, filter);
      scannerId = super.addScanner(s);
      return scannerId;
    } catch (IOException e) {
      LOG.error("Error opening scanner (fsOk: " + this.fsOk + ")",
          RemoteExceptionHandler.checkIOException(e));
      checkFileSystem();
      throw e;
    }
  }

  public void beginTransaction(final long transactionId, final byte[] regionName)
      throws IOException {
    getTransactionalRegion(regionName).beginTransaction(transactionId);
  }

}
