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
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.LogRollListener;
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
class LogRoller extends Thread implements LogRollListener {
  static final Log LOG = LogFactory.getLog(LogRoller.class);
  private final ReentrantLock rollLock = new ReentrantLock();
  private final AtomicBoolean rollLog = new AtomicBoolean(false);
  private final HRegionServer server;
  private volatile long lastrolltime = System.currentTimeMillis();
  // Period to roll log.
  private final long rollperiod;

  /** @param server */
  public LogRoller(final HRegionServer server) {
    super();
    this.server = server;
    this.rollperiod =
      this.server.conf.getLong("hbase.regionserver.logroll.period", 3600000);
  }

  @Override
  public void run() {
    while (!server.isStopRequested()) {
      long now = System.currentTimeMillis();
      boolean periodic = false;
      if (!rollLog.get()) {
        periodic = (now - this.lastrolltime) > this.rollperiod;
        if (!periodic) {
          synchronized (rollLog) {
            try {
              rollLog.wait(server.threadWakeFrequency);
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
        byte [][] regionsToFlush = server.getLog().rollWriter();
        if (regionsToFlush != null) {
          for (byte [] r: regionsToFlush) scheduleFlush(r);
        }
      } catch (FailedLogCloseException e) {
        LOG.fatal("Forcing server shutdown", e);
        server.checkFileSystem();
        server.abort("Failed log close in log roller", e);
      } catch (java.net.ConnectException e) {
        LOG.fatal("Forcing server shutdown", e);
        server.checkFileSystem();
        server.abort("Failed connect in log roller", e);
      } catch (IOException ex) {
        LOG.fatal("Log rolling failed with ioe: ",
          RemoteExceptionHandler.checkIOException(ex));
        server.checkFileSystem();
        // Abort if we get here.  We probably won't recover an IOE. HBASE-1132
        server.abort("IOE in log roller", ex);
      } catch (Exception ex) {
        LOG.error("Log rolling failed", ex);
        server.checkFileSystem();
        server.abort("Log rolling failed", ex);
      } finally {
        rollLog.set(false);
        rollLock.unlock();
      }
    }
    LOG.info("LogRoller exiting.");
  }

  private void scheduleFlush(final byte [] region) {
    boolean scheduled = false;
    HRegion r = this.server.getOnlineRegion(region);
    FlushRequester requester = null;
    if (r != null) {
      requester = this.server.getFlushRequester();
      if (requester != null) {
        requester.request(r);
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
}
