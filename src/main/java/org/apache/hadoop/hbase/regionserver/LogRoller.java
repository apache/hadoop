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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.regionserver.wal.WALObserver;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Runs periodically to determine if the HLog should be rolled.
 *
 * NOTE: This class extends Thread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore
 * sleep time which is invariant.
 */
class LogRoller extends Thread implements WALObserver {
  static final Log LOG = LogFactory.getLog(LogRoller.class);
  private final ReentrantLock rollLock = new ReentrantLock();
  private final AtomicBoolean rollLog = new AtomicBoolean(false);
  private final Server server;
  private final RegionServerServices services;
  private volatile long lastrolltime = System.currentTimeMillis();
  // Period to roll log.
  private final long rollperiod;
  private final int threadWakeFrequency;

  /** @param server */
  public LogRoller(final Server server, final RegionServerServices services) {
    super();
    this.server = server;
    this.services = services;
    this.rollperiod = this.server.getConfiguration().
      getLong("hbase.regionserver.logroll.period", 3600000);
    this.threadWakeFrequency = this.server.getConfiguration().
      getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
  }

  @Override
  public void run() {
    while (!server.isStopped()) {
      long now = System.currentTimeMillis();
      boolean periodic = false;
      if (!rollLog.get()) {
        periodic = (now - this.lastrolltime) > this.rollperiod;
        if (!periodic) {
          synchronized (rollLog) {
            try {
              rollLog.wait(this.threadWakeFrequency);
            } catch (InterruptedException e) {
              // Fall through
            }
          }
          continue;
        }
        // Time for periodic roll
        if (LOG.isDebugEnabled()) {
          LOG.debug("Hlog roll period " + this.rollperiod + "ms elapsed");
        }
      }
      rollLock.lock(); // FindBugs UL_UNRELEASED_LOCK_EXCEPTION_PATH
      try {
        this.lastrolltime = now;
        // This is array of actual region names.
        byte [][] regionsToFlush = this.services.getWAL().rollWriter();
        if (regionsToFlush != null) {
          for (byte [] r: regionsToFlush) scheduleFlush(r);
        }
      } catch (FailedLogCloseException e) {
        server.abort("Failed log close in log roller", e);
      } catch (java.net.ConnectException e) {
        server.abort("Failed log close in log roller", e);
      } catch (IOException ex) {
        // Abort if we get here.  We probably won't recover an IOE. HBASE-1132
        server.abort("IOE in log roller",
          RemoteExceptionHandler.checkIOException(ex));
      } catch (Exception ex) {
        LOG.error("Log rolling failed", ex);
        server.abort("Log rolling failed", ex);
      } finally {
        rollLog.set(false);
        rollLock.unlock();
      }
    }
    LOG.info("LogRoller exiting.");
  }

  /**
   * @param region Encoded name of region to flush.
   */
  private void scheduleFlush(final byte [] region) {
    boolean scheduled = false;
    HRegion r = this.services.getFromOnlineRegions(Bytes.toString(region));
    FlushRequester requester = null;
    if (r != null) {
      requester = this.services.getFlushRequester();
      if (requester != null) {
        requester.requestFlush(r);
        scheduled = true;
      }
    }
    if (!scheduled) {
    LOG.warn("Failed to schedule flush of " +
      Bytes.toString(region) + "r=" + r + ", requester=" + requester);
    }
  }

  public void logRollRequested() {
    synchronized (rollLog) {
      rollLog.set(true);
      rollLog.notifyAll();
    }
  }

  /**
   * Called by region server to wake up this thread if it sleeping.
   * It is sleeping if rollLock is not held.
   */
  public void interruptIfNecessary() {
    try {
      rollLock.lock();
      this.interrupt();
    } finally {
      rollLock.unlock();
    }
  }

  @Override
  public void logRolled(Path newFile) {
    // Not interested
  }

  @Override
  public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey,
      WALEdit logEdit) {
    // Not interested.
  }

  @Override
  public void visitLogEntryBeforeWrite(HTableDescriptor htd, HLogKey logKey,
                                       WALEdit logEdit) {
    //Not interested
  }

  @Override
  public void logCloseRequested() {
    // not interested
  }
}
