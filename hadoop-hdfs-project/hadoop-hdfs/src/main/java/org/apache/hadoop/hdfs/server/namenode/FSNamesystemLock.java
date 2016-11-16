
/**
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

package org.apache.hadoop.hdfs.server.namenode;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Timer;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LOCK_DETAILED_METRICS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY;

/**
 * Mimics a ReentrantReadWriteLock but does not directly implement the interface
 * so more sophisticated locking capabilities and logging/metrics are possible.
 * {@link org.apache.hadoop.hdfs.DFSConfigKeys#DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY}
 * to be true, metrics will be emitted into the FSNamesystem metrics registry
 * for each operation which acquires this lock indicating how long the operation
 * held the lock for. Note that if a thread dies, metrics produced after the
 * most recent snapshot will be lost due to the use of
 * {@link MutableRatesWithAggregation}. However since threads are re-used
 * between operations this should not generally be an issue.
 */
class FSNamesystemLock {
  @VisibleForTesting
  protected ReentrantReadWriteLock coarseLock;

  private final boolean metricsEnabled;
  private final MutableRatesWithAggregation detailedHoldTimeMetrics;
  private final Timer timer;

  /**
   * Log statements about long lock hold times will not be produced more
   * frequently than this interval.
   */
  private final long lockSuppressWarningInterval;

  /** Threshold (ms) for long holding write lock report. */
  private final long writeLockReportingThreshold;
  /** Last time stamp for write lock. Keep the longest one for multi-entrance.*/
  private long writeLockHeldTimeStamp;
  private int numWriteLockWarningsSuppressed = 0;
  private long timeStampOfLastWriteLockReport = 0;
  private long longestWriteLockHeldInterval = 0;

  /** Threshold (ms) for long holding read lock report. */
  private final long readLockReportingThreshold;
  /**
   * Last time stamp for read lock. Keep the longest one for
   * multi-entrance. This is ThreadLocal since there could be
   * many read locks held simultaneously.
   */
  private final ThreadLocal<Long> readLockHeldTimeStamp =
      new ThreadLocal<Long>() {
        @Override
        public Long initialValue() {
          return Long.MAX_VALUE;
        }
      };
  private final AtomicInteger numReadLockWarningsSuppressed =
      new AtomicInteger(0);
  private final AtomicLong timeStampOfLastReadLockReport = new AtomicLong(0);
  private final AtomicLong longestReadLockHeldInterval = new AtomicLong(0);

  @VisibleForTesting
  static final String OP_NAME_OTHER = "OTHER";
  private static final String READ_LOCK_METRIC_PREFIX = "FSNReadLock";
  private static final String WRITE_LOCK_METRIC_PREFIX = "FSNWriteLock";

  FSNamesystemLock(Configuration conf,
      MutableRatesWithAggregation detailedHoldTimeMetrics) {
    this(conf, detailedHoldTimeMetrics, new Timer());
  }

  @VisibleForTesting
  FSNamesystemLock(Configuration conf,
      MutableRatesWithAggregation detailedHoldTimeMetrics, Timer timer) {
    boolean fair = conf.getBoolean("dfs.namenode.fslock.fair", true);
    FSNamesystem.LOG.info("fsLock is fair: " + fair);
    this.coarseLock = new ReentrantReadWriteLock(fair);
    this.timer = timer;

    this.writeLockReportingThreshold = conf.getLong(
        DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY,
        DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_DEFAULT);
    this.readLockReportingThreshold = conf.getLong(
        DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY,
        DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT);
    this.lockSuppressWarningInterval = conf.getTimeDuration(
        DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY,
        DFS_LOCK_SUPPRESS_WARNING_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    this.metricsEnabled = conf.getBoolean(
        DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY,
        DFS_NAMENODE_LOCK_DETAILED_METRICS_DEFAULT);
    FSNamesystem.LOG.info("Detailed lock hold time metrics enabled: " +
        this.metricsEnabled);
    this.detailedHoldTimeMetrics = detailedHoldTimeMetrics;
  }

  public void readLock() {
    coarseLock.readLock().lock();
    if (coarseLock.getReadHoldCount() == 1) {
      readLockHeldTimeStamp.set(timer.monotonicNow());
    }
  }

  public void readUnlock() {
    readUnlock(OP_NAME_OTHER);
  }

  public void readUnlock(String opName) {
    final boolean needReport = coarseLock.getReadHoldCount() == 1;
    final long readLockInterval =
        timer.monotonicNow() - readLockHeldTimeStamp.get();
    coarseLock.readLock().unlock();

    if (needReport) {
      addMetric(opName, readLockInterval, false);
      readLockHeldTimeStamp.remove();
    }
    if (needReport && readLockInterval >= this.readLockReportingThreshold) {
      long localLongestReadLock;
      do {
        localLongestReadLock = longestReadLockHeldInterval.get();
      } while (localLongestReadLock - readLockInterval < 0 &&
          !longestReadLockHeldInterval.compareAndSet(localLongestReadLock,
              readLockInterval));

      long localTimeStampOfLastReadLockReport;
      long now;
      do {
        now = timer.monotonicNow();
        localTimeStampOfLastReadLockReport =
            timeStampOfLastReadLockReport.get();
        if (now - localTimeStampOfLastReadLockReport <
            lockSuppressWarningInterval) {
          numReadLockWarningsSuppressed.incrementAndGet();
          return;
        }
      } while (!timeStampOfLastReadLockReport.compareAndSet(
          localTimeStampOfLastReadLockReport, now));
      int numSuppressedWarnings = numReadLockWarningsSuppressed.getAndSet(0);
      long longestLockHeldInterval = longestReadLockHeldInterval.getAndSet(0);
      FSNamesystem.LOG.info("FSNamesystem read lock held for " +
          readLockInterval + " ms via\n" +
          StringUtils.getStackTrace(Thread.currentThread()) +
          "\tNumber of suppressed read-lock reports: " + numSuppressedWarnings +
          "\n\tLongest read-lock held interval: " + longestLockHeldInterval);
    }
  }
  
  public void writeLock() {
    coarseLock.writeLock().lock();
    if (coarseLock.getWriteHoldCount() == 1) {
      writeLockHeldTimeStamp = timer.monotonicNow();
    }
  }

  public void writeLockInterruptibly() throws InterruptedException {
    coarseLock.writeLock().lockInterruptibly();
    if (coarseLock.getWriteHoldCount() == 1) {
      writeLockHeldTimeStamp = timer.monotonicNow();
    }
  }

  public void writeUnlock() {
    writeUnlock(OP_NAME_OTHER);
  }

  public void writeUnlock(String opName) {
    final boolean needReport = coarseLock.getWriteHoldCount() == 1 &&
        coarseLock.isWriteLockedByCurrentThread();
    final long currentTime = timer.monotonicNow();
    final long writeLockInterval = currentTime - writeLockHeldTimeStamp;

    boolean logReport = false;
    int numSuppressedWarnings = 0;
    long longestLockHeldInterval = 0;
    if (needReport && writeLockInterval >= this.writeLockReportingThreshold) {
      if (writeLockInterval > longestWriteLockHeldInterval) {
        longestWriteLockHeldInterval = writeLockInterval;
      }
      if (currentTime - timeStampOfLastWriteLockReport >
          this.lockSuppressWarningInterval) {
        logReport = true;
        numSuppressedWarnings = numWriteLockWarningsSuppressed;
        numWriteLockWarningsSuppressed = 0;
        longestLockHeldInterval = longestWriteLockHeldInterval;
        longestWriteLockHeldInterval = 0;
        timeStampOfLastWriteLockReport = currentTime;
      } else {
        numWriteLockWarningsSuppressed++;
      }
    }

    coarseLock.writeLock().unlock();

    if (needReport) {
      addMetric(opName, writeLockInterval, true);
    }

    if (logReport) {
      FSNamesystem.LOG.info("FSNamesystem write lock held for " +
          writeLockInterval + " ms via\n" +
          StringUtils.getStackTrace(Thread.currentThread()) +
          "\tNumber of suppressed write-lock reports: " +
          numSuppressedWarnings + "\n\tLongest write-lock held interval: " +
          longestLockHeldInterval);
    }
  }

  public int getReadHoldCount() {
    return coarseLock.getReadHoldCount();
  }
  
  public int getWriteHoldCount() {
    return coarseLock.getWriteHoldCount();
  }
  
  public boolean isWriteLockedByCurrentThread() {
    return coarseLock.isWriteLockedByCurrentThread();
  }

  public Condition newWriteLockCondition() {
    return coarseLock.writeLock().newCondition();
  }

  /**
   * Returns the QueueLength of waiting threads.
   *
   * A larger number indicates greater lock contention.
   *
   * @return int - Number of threads waiting on this lock
   */
  public int getQueueLength() {
    return coarseLock.getQueueLength();
  }

  /**
   * Add the lock hold time for a recent operation to the metrics.
   * @param operationName Name of the operation for which to record the time
   * @param value Length of time the lock was held
   */
  private void addMetric(String operationName, long value, boolean isWrite) {
    if (metricsEnabled) {
      String metricName =
          (isWrite ? WRITE_LOCK_METRIC_PREFIX : READ_LOCK_METRIC_PREFIX) +
          org.apache.commons.lang.StringUtils.capitalize(operationName);
      detailedHoldTimeMetrics.add(metricName, value);
    }
  }

}
