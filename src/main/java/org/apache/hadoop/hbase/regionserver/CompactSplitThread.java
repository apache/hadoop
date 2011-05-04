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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

/**
 * Compact region on request and then run split if appropriate
 */
public class CompactSplitThread extends Thread implements CompactionRequestor {
  static final Log LOG = LogFactory.getLog(CompactSplitThread.class);
  private final long frequency;
  private final ReentrantLock lock = new ReentrantLock();

  private final HRegionServer server;
  private final Configuration conf;

  private final PriorityCompactionQueue compactionQueue =
    new PriorityCompactionQueue();

  /* The default priority for user-specified compaction requests.
   * The user gets top priority unless we have blocking compactions. (Pri <= 0)
   */
  public static final int PRIORITY_USER = 1;

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
    this.conf = server.getConfiguration();
    this.regionSplitLimit = conf.getInt("hbase.regionserver.regionSplitLimit",
        Integer.MAX_VALUE);
    this.frequency =
      conf.getLong("hbase.regionserver.thread.splitcompactcheckfrequency",
      20 * 1000);
  }

  @Override
  public void run() {
    while (!this.server.isStopped()) {
      CompactionRequest compactionRequest = null;
      HRegion r = null;
      try {
        compactionRequest = compactionQueue.poll(this.frequency, TimeUnit.MILLISECONDS);
        if (compactionRequest != null) {
          lock.lock();
          try {
            if(!this.server.isStopped()) {
              // Don't interrupt us while we are working
              r = compactionRequest.getHRegion();
              byte [] midKey = r.compactStore(compactionRequest.getStore());
              if (r.getLastCompactInfo() != null) {  // compaction aborted?
                this.server.getMetrics().addCompaction(r.getLastCompactInfo());
              }
              if (shouldSplitRegion() && midKey != null &&
                  !this.server.isStopped()) {
                split(r, midKey);
              }
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
    compactionQueue.clear();
    LOG.info(getName() + " exiting");
  }

  public synchronized void requestCompaction(final HRegion r,
      final String why) {
    for(Store s : r.getStores().values()) {
      requestCompaction(r, s, false, why, s.getCompactPriority());
    }
  }

  public synchronized void requestCompaction(final HRegion r,
      final String why, int p) {
    requestCompaction(r, false, why, p);
  }

  public synchronized void requestCompaction(final HRegion r,
      final boolean force, final String why, int p) {
    for(Store s : r.getStores().values()) {
      requestCompaction(r, s, force, why, p);
    }
  }

  /**
   * @param r HRegion store belongs to
   * @param force Whether next compaction should be major
   * @param why Why compaction requested -- used in debug messages
   */
  public synchronized void requestCompaction(final HRegion r, final Store s,
      final boolean force, final String why, int priority) {
    if (this.server.isStopped()) {
      return;
    }
    // tell the region to major-compact (and don't downgrade it)
    if (force) {
      s.setForceMajorCompaction(force);
    }
    CompactionRequest compactionRequest = new CompactionRequest(r, s, priority);
    if (compactionQueue.add(compactionRequest) && LOG.isDebugEnabled()) {
      LOG.debug("Compaction " + (force? "(major) ": "") +
        "requested for region " + r.getRegionNameAsString() +
        "/" + r.getRegionInfo().getEncodedName() +
        ", store " + s +
        (why != null && !why.isEmpty()? " because " + why: "") +
        "; priority=" + priority + ", compaction queue size=" + compactionQueue.size());
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
      st.execute(this.server, this.server);
    } catch (Exception e) {
      try {
        LOG.info("Running rollback of failed split of " +
          parent.getRegionNameAsString() + "; " + e.getMessage());
        st.rollback(this.server, this.server);
        LOG.info("Successful rollback of failed split of " +
          parent.getRegionNameAsString());
      } catch (Exception ee) {
        // If failed rollback, kill this server to avoid having a hole in table.
        LOG.info("Failed rollback of failed split of " +
          parent.getRegionNameAsString() + " -- aborting server", ee);
        this.server.abort("Failed split");
      }
      return;
    }

    LOG.info("Region split, META updated, and report to master. Parent=" +
      parent.getRegionInfo().getRegionNameAsString() + ", new regions: " +
      st.getFirstDaughter().getRegionNameAsString() + ", " +
      st.getSecondDaughter().getRegionNameAsString() + ". Split took " +
      StringUtils.formatTimeDiff(System.currentTimeMillis(), startTime));
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    if (lock.tryLock()) {
      try {
        this.interrupt();
      } finally {
        lock.unlock();
      }
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
