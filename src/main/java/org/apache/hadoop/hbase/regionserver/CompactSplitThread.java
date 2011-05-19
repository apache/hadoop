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

import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;

import com.google.common.base.Preconditions;

/**
 * Compact region on request and then run split if appropriate
 */
public class CompactSplitThread implements CompactionRequestor {
  static final Log LOG = LogFactory.getLog(CompactSplitThread.class);

  private final HRegionServer server;
  private final Configuration conf;

  private final ThreadPoolExecutor largeCompactions;
  private final ThreadPoolExecutor smallCompactions;
  private final ThreadPoolExecutor splits;
  private final long throttleSize;

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
  CompactSplitThread(HRegionServer server) {
    super();
    this.server = server;
    this.conf = server.getConfiguration();
    this.regionSplitLimit = conf.getInt("hbase.regionserver.regionSplitLimit",
        Integer.MAX_VALUE);

    int largeThreads = Math.max(1, conf.getInt(
        "hbase.regionserver.thread.compaction.large", 1));
    int smallThreads = conf.getInt(
        "hbase.regionserver.thread.compaction.small", 1);
    if (conf.get("hbase.regionserver.thread.compaction.throttle") != null) {
      throttleSize = conf.getLong(
          "hbase.regionserver.thread.compaction.throttle", 0);
    } else {
      // we have a complicated default. see HBASE-3877
      long flushSize = conf.getLong("hbase.hregion.memstore.flush.size",
          HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE);
      long splitSize = conf.getLong("hbase.hregion.max.filesize",
          HConstants.DEFAULT_MAX_FILE_SIZE);
      throttleSize = Math.min(flushSize * 2, splitSize / 2);
    }

    int splitThreads = conf.getInt("hbase.regionserver.thread.split", 1);

    // if we have throttle threads, make sure the user also specified size
    Preconditions.checkArgument(smallThreads == 0 || throttleSize > 0);

    this.largeCompactions = new ThreadPoolExecutor(largeThreads, largeThreads,
        60, TimeUnit.SECONDS, new PriorityBlockingQueue<Runnable>());
    this.largeCompactions
        .setRejectedExecutionHandler(new CompactionRequest.Rejection());
    if (smallThreads <= 0) {
      this.smallCompactions = null;
    } else {
      this.smallCompactions = new ThreadPoolExecutor(smallThreads, smallThreads,
          60, TimeUnit.SECONDS, new PriorityBlockingQueue<Runnable>());
      this.smallCompactions
          .setRejectedExecutionHandler(new CompactionRequest.Rejection());
    }
    this.splits = (ThreadPoolExecutor) Executors
        .newFixedThreadPool(splitThreads);
  }

  @Override
  public String toString() {
    return "compaction_queue="
        + (smallCompactions != null ? "("
            + largeCompactions.getQueue().size() + ":"
            + smallCompactions.getQueue().size() + ")"
            : largeCompactions.getQueue().size())
        + ", split_queue=" + splits.getQueue().size();
  }

  public synchronized boolean requestSplit(final HRegion r) {
    // don't split regions that are blocking
    if (shouldSplitRegion() && r.getCompactPriority() >= PRIORITY_USER) {
      byte[] midKey = r.checkSplit();
      if (midKey != null) {
        requestSplit(r, midKey);
        return true;
      }
    }
    return false;
  }

  public synchronized void requestSplit(final HRegion r, byte[] midKey) {
    if (midKey == null) {
      LOG.debug("Region " + r.getRegionNameAsString() +
        " not splittable because midkey=null");
      return;
    }
    try {
      this.splits.execute(new SplitRequest(r, midKey, this.server));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Split requested for " + r + ".  " + this);
      }
    } catch (RejectedExecutionException ree) {
      LOG.info("Could not execute split for " + r, ree);
    }
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
   * @param s Store to request compaction on
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
      cr.setServer(server);
      if (priority != NO_PRIORITY) {
        cr.setPriority(priority);
      }
      ThreadPoolExecutor pool = largeCompactions;
      if (smallCompactions != null && throttleSize > cr.getSize()) {
        // smallCompactions is like the 10 items or less line at Walmart
        pool = smallCompactions;
      }
      pool.execute(cr);
      if (LOG.isDebugEnabled()) {
        String type = "";
        if (smallCompactions != null) {
          type = (pool == smallCompactions) ? "Small " : "Large ";
        }
        LOG.debug(type + "Compaction requested: " + cr
            + (why != null && !why.isEmpty() ? "; Because: " + why : "")
            + "; " + this);
      }
    }
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    splits.shutdown();
    largeCompactions.shutdown();
    if (smallCompactions != null)
      smallCompactions.shutdown();
  }

  private void waitFor(ThreadPoolExecutor t, String name) {
    boolean done = false;
    while (!done) {
      try {
        done = t.awaitTermination(60, TimeUnit.SECONDS);
        LOG.debug("Waiting for " + name + " to finish...");
      } catch (InterruptedException ie) {
        LOG.debug("Interrupted waiting for " + name + " to finish...");
      }
    }
  }

  void join() {
    waitFor(splits, "Split Thread");
    waitFor(largeCompactions, "Large Compaction Thread");
    if (smallCompactions != null) {
      waitFor(smallCompactions, "Small Compaction Thread");
    }
  }

  /**
   * Returns the current size of the queue containing regions that are
   * processed.
   *
   * @return The current size of the regions queue.
   */
  public int getCompactionQueueSize() {
    int size = largeCompactions.getQueue().size();
    if (smallCompactions != null)
      size += smallCompactions.getQueue().size();
    return size;
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
