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

  /** The priorities for a compaction request. */
  public enum Priority implements Comparable<Priority> {
    //NOTE: All priorities should be numbered consecutively starting with 1.
    //The highest priority should be 1 followed by all lower priorities.
    //Priorities can be changed at anytime without requiring any changes to the
    //queue.

    /** HIGH_BLOCKING should only be used when an operation is blocked until a
     * compact / split is done (e.g. a MemStore can't flush because it has
     * "too many store files" and is blocking until a compact / split is done)
     */
    HIGH_BLOCKING(1),
    /** A normal compaction / split request */
    NORMAL(2),
    /** A low compaction / split request -- not currently used */
    LOW(3);

    int value;

    Priority(int value) {
      this.value = value;
    }

    int getInt() {
      return value;
    }
  }

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
      HRegion r = null;
      try {
        r = compactionQueue.poll(this.frequency, TimeUnit.MILLISECONDS);
        if (r != null && !this.server.isStopped()) {
          lock.lock();
          try {
            // Don't interrupt us while we are working
            byte [] midKey = r.compactStores();
            if (shouldSplitRegion() && midKey != null &&
                !this.server.isStopped()) {
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
    compactionQueue.clear();
    LOG.info(getName() + " exiting");
  }

  public synchronized void requestCompaction(final HRegion r,
      final String why) {
    requestCompaction(r, false, why, Priority.NORMAL);
  }

  public synchronized void requestCompaction(final HRegion r,
      final String why, Priority p) {
    requestCompaction(r, false, why, p);
  }

  public synchronized void requestCompaction(final HRegion r,
      final boolean force, final String why) {
    requestCompaction(r, force, why, Priority.NORMAL);
  }

  /**
   * @param r HRegion store belongs to
   * @param force Whether next compaction should be major
   * @param why Why compaction requested -- used in debug messages
   */
  public synchronized void requestCompaction(final HRegion r,
      final boolean force, final String why, Priority priority) {
    if (this.server.isStopped()) {
      return;
    }
    r.setForceMajorCompaction(force);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Compaction " + (force? "(major) ": "") +
        "requested for region " + r.getRegionNameAsString() +
        (why != null && !why.isEmpty()? " because: " + why: "") +
        "; Priority: " + priority + "; Compaction queue size: " + compactionQueue.size());
    }
    compactionQueue.add(r, priority);
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
