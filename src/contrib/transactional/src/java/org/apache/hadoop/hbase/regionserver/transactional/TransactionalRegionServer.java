/**
 * Copyright 2009 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.transactional.TransactionalRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;
import org.apache.hadoop.hbase.regionserver.HLog;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
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
  
  static {
    TransactionalRPC.initialize();
  }

  private static final String LEASE_TIME = "hbase.transaction.leasetime";
  private static final int DEFAULT_LEASE_TIME = 60 * 1000;
  private static final int LEASE_CHECK_FREQUENCY = 1000;
  
  static final Log LOG = LogFactory.getLog(TransactionalRegionServer.class);
  private final Leases transactionLeases;
  private final CleanOldTransactionsChore cleanOldTransactionsThread;

  /**
   * @param conf
   * @throws IOException
   */
  public TransactionalRegionServer(final HBaseConfiguration conf)
      throws IOException {
    super(conf);
    cleanOldTransactionsThread = new CleanOldTransactionsChore(this,
        super.stopRequested);
    transactionLeases = new Leases(conf.getInt(LEASE_TIME, DEFAULT_LEASE_TIME),
        LEASE_CHECK_FREQUENCY);
    LOG.error("leases time:"+conf.getInt(LEASE_TIME, DEFAULT_LEASE_TIME));
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
    Threads.setDaemonThreadRunning(this.transactionLeases, "Transactional leases");

  }

  @Override
  protected HLog instantiateHLog(Path logdir) throws IOException {
    HLog newlog = new THLog(super.getFileSystem(), logdir, conf, super.getLogRoller());
    return newlog;
  }
  
  @Override
  protected HRegion instantiateRegion(final HRegionInfo regionInfo)
      throws IOException {
    HRegion r = new TransactionalRegion(HTableDescriptor.getTableDir(super
        .getRootDir(), regionInfo.getTableDesc().getName()), super.hlog, super
        .getFileSystem(), super.conf, regionInfo, super.getFlushRequester(), this.getTransactionalLeases());
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
  
  protected Leases getTransactionalLeases() {
    return this.transactionLeases;
  }

  /** We want to delay the close region for a bit if we have commit pending transactions.
   * 
   */
  @Override
  protected void closeRegion(final HRegionInfo hri, final boolean reportWhenCompleted)
  throws IOException {
    getTransactionalRegion(hri.getRegionName()).prepareToClose();
    super.closeRegion(hri, reportWhenCompleted);
  }
  
  public void abort(final byte[] regionName, final long transactionId)
      throws IOException {
    checkOpen();
    super.getRequestCount().incrementAndGet();
    try {
      getTransactionalRegion(regionName).abort(transactionId);
    } catch(NotServingRegionException e) {
      LOG.info("Got not serving region durring abort. Ignoring.");
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

  public int commitRequest(final byte[] regionName, final long transactionId)
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
  
  public boolean commitIfPossible(byte[] regionName, long transactionId)
  throws IOException {
    checkOpen();
    super.getRequestCount().incrementAndGet();
    try {
      return getTransactionalRegion(regionName).commitIfPossible(transactionId);
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  public long openScanner(final long transactionId, byte [] regionName, Scan scan)
  throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if (regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if (scan == null) {
      npe = new NullPointerException("scan is null");
    }
    if (npe != null) {
      throw new IOException("Invalid arguments to openScanner", npe);
    }
    super.getRequestCount().incrementAndGet();
    try {
      TransactionalRegion r = getTransactionalRegion(regionName);
      InternalScanner s = r.getScanner(transactionId, scan);
      long scannerId = addScanner(s);
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

  public void delete(long transactionId, byte[] regionName, Delete delete)
      throws IOException {
    getTransactionalRegion(regionName).delete(transactionId, delete);
  }

  public Result get(long transactionId, byte[] regionName, Get get)
      throws IOException {
    return getTransactionalRegion(regionName).get(transactionId, get);
  }

  public void put(long transactionId, byte[] regionName, Put put)
      throws IOException {
    getTransactionalRegion(regionName).put(transactionId, put);
    
  }

  public int put(long transactionId, byte[] regionName, Put[] puts)
      throws IOException {
    getTransactionalRegion(regionName).put(transactionId, puts);
    return puts.length; // ??
  }
}
