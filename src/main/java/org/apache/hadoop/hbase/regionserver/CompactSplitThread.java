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

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.util.StringUtils;

/**
 * Compact region on request and then run split if appropriate
 */
class CompactSplitThread extends Thread {
  static final Log LOG = LogFactory.getLog(CompactSplitThread.class);

  private final long frequency;
  private final ReentrantLock lock = new ReentrantLock();

  private final HRegionServer server;
  private final Configuration conf;

  private final BlockingQueue<HRegion> compactionQueue =
    new LinkedBlockingQueue<HRegion>();

  private final HashSet<HRegion> regionsInQueue = new HashSet<HRegion>();

  /**
   * Splitting should not take place if the total number of regions exceed this.
   * This is not a hard limit to the number of regions but it is a guideline to
   * stop splitting after number of online regions is greater than this.
   */
  private int regionSplitLimit;

  /** @param server */
  public CompactSplitThread(HRegionServer server) {
    super();
    this.server = server;
    this.conf = server.conf;
    this.regionSplitLimit = conf.getInt("hbase.regionserver.regionSplitLimit",
        Integer.MAX_VALUE);
    this.frequency =
      conf.getLong("hbase.regionserver.thread.splitcompactcheckfrequency",
      20 * 1000);
  }

  @Override
  public void run() {
    while (!this.server.isStopRequested()) {
      HRegion r = null;
      try {
        r = compactionQueue.poll(this.frequency, TimeUnit.MILLISECONDS);
        if (r != null && !this.server.isStopRequested()) {
          synchronized (regionsInQueue) {
            regionsInQueue.remove(r);
          }
          lock.lock();
          try {
            // Don't interrupt us while we are working
            byte [] midKey = r.compactStores();
            if (shouldSplitRegion() && midKey != null &&
                !this.server.isStopRequested()) {
              split(r, midKey);
            }
          } finally {
            lock.unlock();
          }
        }
      } catch (InterruptedException ex) {
        continue;
      } catch (IOException ex) {
        LOG.error("Compaction/Split failed for region " +
            r.getRegionNameAsString(),
          RemoteExceptionHandler.checkIOException(ex));
        if (!server.checkFileSystem()) {
          break;
        }
      } catch (Exception ex) {
        LOG.error("Compaction failed" +
            (r != null ? (" for region " + r.getRegionNameAsString()) : ""),
            ex);
        if (!server.checkFileSystem()) {
          break;
        }
      }
    }
    regionsInQueue.clear();
    compactionQueue.clear();
    LOG.info(getName() + " exiting");
  }

  /**
   * @param r HRegion store belongs to
   * @param why Why compaction requested -- used in debug messages
   */
  public synchronized void compactionRequested(final HRegion r,
      final String why) {
    compactionRequested(r, false, why);
  }

  /**
   * @param r HRegion store belongs to
   * @param force Whether next compaction should be major
   * @param why Why compaction requested -- used in debug messages
   */
  public synchronized void compactionRequested(final HRegion r,
      final boolean force, final String why) {
    if (this.server.stopRequested.get()) {
      return;
    }
    r.setForceMajorCompaction(force);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Compaction " + (force? "(major) ": "") +
        "requested for region " + r.getRegionNameAsString() +
        (why != null && !why.isEmpty()? " because: " + why: ""));
    }
    synchronized (regionsInQueue) {
      if (!regionsInQueue.contains(r)) {
        compactionQueue.add(r);
        regionsInQueue.add(r);
      }
    }
  }

  private void split(final HRegion parent, final byte [] midKey)
  throws IOException {
    final long startTime = System.currentTimeMillis();
    SplitTransaction st = new SplitTransaction(parent, midKey);
    // If prepare does not return true, for some reason -- logged inside in
    // the prepare call -- we are not ready to split just now.  Just return.
    if (!st.prepare()) return;
    try {
      st.execute(this.server);
    } catch (IOException ioe) {
      try {
        LOG.info("Running rollback of failed split of " +
          parent.getRegionNameAsString() + "; " + ioe.getMessage());
        st.rollback(this.server);
        LOG.info("Successful rollback of failed split of " +
          parent.getRegionNameAsString());
      } catch (RuntimeException e) {
        // If failed rollback, kill this server to avoid having a hole in table.
        LOG.info("Failed rollback of failed split of " +
          parent.getRegionNameAsString() + " -- aborting server", e);
        this.server.abort("Failed split");
      }
      return;
    }

    // Now tell the master about the new regions.  If we fail here, its OK.
    // Basescanner will do fix up.  And reporting split to master is going away.
    // TODO: Verify this still holds in new master rewrite.
    this.server.reportSplit(parent.getRegionInfo(), st.getFirstDaughter(),
      st.getSecondDaughter());
    LOG.info("Region split, META updated, and report to master. Parent=" +
      parent.getRegionInfo() + ", new regions: " +
      st.getFirstDaughter() + ", " + st.getSecondDaughter() + ". Split took " +
      StringUtils.formatTimeDiff(System.currentTimeMillis(), startTime));
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    if (lock.tryLock()) {
      this.interrupt();
    }
  }

  /**
   * Returns the current size of the queue containing regions that are
   * processed.
   *
   * @return The current size of the regions queue.
   */
  public int getCompactionQueueSize() {
    return compactionQueue.size();
  }

  private boolean shouldSplitRegion() {
    return (regionSplitLimit > server.getNumberOfOnlineRegions());
  }

  /**
   * @return the regionSplitLimit
   */
  public int getRegionSplitLimit() {
    return this.regionSplitLimit;
  }
}
