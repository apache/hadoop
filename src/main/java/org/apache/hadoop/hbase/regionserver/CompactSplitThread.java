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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
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

  protected final BlockingQueue<CompactionRequest> compactionQueue =
    new PriorityBlockingQueue<CompactionRequest>();

  /* The default priority for user-specified compaction requests.
   * The user gets top priority unless we have blocking compactions. (Pri <= 0)
   */
  public static final int PRIORITY_USER = 1;
  public static final int NO_PRIORITY = Integer.MIN_VALUE;

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
      boolean completed = false;
      try {
        compactionRequest = compactionQueue.poll(this.frequency, TimeUnit.MILLISECONDS);
        if (compactionRequest != null) {
          r = compactionRequest.getHRegion();
          lock.lock();
          try {
            // look for a split first
            if(!this.server.isStopped()) {
              // don't split regions that are blocking
              if (shouldSplitRegion() && r.getCompactPriority() >= PRIORITY_USER) {
                byte[] midkey = compactionRequest.getStore().checkSplit();
                if (midkey != null) {
                  split(r, midkey);
                  continue;
                }
              }
            }

            // now test for compaction
            if(!this.server.isStopped()) {
              long startTime = EnvironmentEdgeManager.currentTimeMillis();
              completed = r.compact(compactionRequest);
              long now = EnvironmentEdgeManager.currentTimeMillis();
              LOG.info(((completed) ? "completed" : "aborted")
                  + " compaction: " + compactionRequest + ", duration="
                  + StringUtils.formatTimeDiff(now, startTime));
              if (completed) { // compaction aborted?
                this.server.getMetrics().
                  addCompaction(now - startTime, compactionRequest.getSize());
              }
            }
          } finally {
            lock.unlock();
          }
        }
      } catch (InterruptedException ex) {
        continue;
      } catch (IOException ex) {
        LOG.error("Compaction/Split failed " + compactionRequest,
          RemoteExceptionHandler.checkIOException(ex));
        if (!server.checkFileSystem()) {
          break;
        }
      } catch (Exception ex) {
        LOG.error("Compaction failed " + compactionRequest, ex);
        if (!server.checkFileSystem()) {
          break;
        }
      } finally {
        if (compactionRequest != null) {
          Store s = compactionRequest.getStore();
          s.finishRequest(compactionRequest);
          // degenerate case: blocked regions require recursive enqueues
          if (s.getCompactPriority() < PRIORITY_USER && completed) {
            requestCompaction(r, s, "Recursive enqueue");
          }
        }
        compactionRequest = null;
      }
    }
    compactionQueue.clear();
    LOG.info(getName() + " exiting");
  }

  public synchronized void requestCompaction(final HRegion r,
      final String why) {
    for(Store s : r.getStores().values()) {
      requestCompaction(r, s, why, NO_PRIORITY);
    }
  }

  public synchronized void requestCompaction(final HRegion r, final Store s,
      final String why) {
    requestCompaction(r, s, why, NO_PRIORITY);
  }

  public synchronized void requestCompaction(final HRegion r, final String why,
      int p) {
    for(Store s : r.getStores().values()) {
      requestCompaction(r, s, why, p);
    }
  }

  /**
   * @param r HRegion store belongs to
   * @param force Whether next compaction should be major
   * @param why Why compaction requested -- used in debug messages
   * @param priority override the default priority (NO_PRIORITY == decide)
   */
  public synchronized void requestCompaction(final HRegion r, final Store s,
      final String why, int priority) {
    if (this.server.isStopped()) {
      return;
    }
    CompactionRequest cr = s.requestCompaction();
    if (cr != null) {
      if (priority != NO_PRIORITY) {
        cr.setPriority(priority);
      }
      boolean addedToQueue = compactionQueue.add(cr);
      if (!addedToQueue) {
        LOG.error("Could not add request to compaction queue: " + cr);
        s.finishRequest(cr);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("Compaction requested: " + cr
            + (why != null && !why.isEmpty() ? "; Because: " + why : "")
            + "; Priority: " + priority + "; Compaction queue size: "
            + compactionQueue.size());
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
