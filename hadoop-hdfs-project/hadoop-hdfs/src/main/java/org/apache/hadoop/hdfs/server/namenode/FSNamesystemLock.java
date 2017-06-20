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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_FSLOCK_FAIR_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_FSLOCK_FAIR_KEY;
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
 * held the lock for. These metrics have names of the form
 * FSN(Read|Write)LockNanosOperationName, where OperationName denotes the name
 * of the operation that initiated the lock hold (this will be OTHER for certain
 * uncategorized operations) and they export the hold time values in
 * nanoseconds. Note that if a thread dies, metrics produced after the
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
  private final long lockSuppressWarningIntervalMs;

  /** Threshold (ms) for long holding write lock report. */
  private final long writeLockReportingThresholdMs;
  /** Last time stamp for write lock. Keep the longest one for multi-entrance.*/
  private long writeLockHeldTimeStampNanos;
  private int numWriteLockWarningsSuppressed = 0;
  /** Time stamp (ms) of the last time a write lock report was written. */
  private long timeStampOfLastWriteLockReportMs = 0;
  /** Longest time (ms) a write lock was held since the last report. */
  private long longestWriteLockHeldIntervalMs = 0;

  /** Threshold (ms) for long holding read lock report. */
  private final long readLockReportingThresholdMs;
  /**
   * Last time stamp for read lock. Keep the longest one for
   * multi-entrance. This is ThreadLocal since there could be
   * many read locks held simultaneously.
   */
  private final ThreadLocal<Long> readLockHeldTimeStampNanos =
      new ThreadLocal<Long>() {
        @Override
        public Long initialValue() {
          return Long.MAX_VALUE;
        }
      };
  private final AtomicInteger numReadLockWarningsSuppressed =
      new AtomicInteger(0);
  /** Time stamp (ms) of the last time a read lock report was written. */
  private final AtomicLong timeStampOfLastReadLockReportMs = new AtomicLong(0);
  /** Longest time (ms) a read lock was held since the last report. */
  private final AtomicLong longestReadLockHeldIntervalMs = new AtomicLong(0);

  @VisibleForTesting
  static final String OP_NAME_OTHER = "OTHER";
  private static final String READ_LOCK_METRIC_PREFIX = "FSNReadLock";
  private static final String WRITE_LOCK_METRIC_PREFIX = "FSNWriteLock";
  private static final String LOCK_METRIC_SUFFIX = "Nanos";

  FSNamesystemLock(Configuration conf,
      MutableRatesWithAggregation detailedHoldTimeMetrics) {
    this(conf, detailedHoldTimeMetrics, new Timer());
  }

  @VisibleForTesting
  FSNamesystemLock(Configuration conf,
      MutableRatesWithAggregation detailedHoldTimeMetrics, Timer timer) {
    boolean fair = conf.getBoolean(DFS_NAMENODE_FSLOCK_FAIR_KEY,
        DFS_NAMENODE_FSLOCK_FAIR_DEFAULT);
    FSNamesystem.LOG.info("fsLock is fair: " + fair);
    this.coarseLock = new ReentrantReadWriteLock(fair);
    this.timer = timer;

    this.writeLockReportingThresholdMs = conf.getLong(
        DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY,
        DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_DEFAULT);
    this.readLockReportingThresholdMs = conf.getLong(
        DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY,
        DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT);
    this.lockSuppressWarningIntervalMs = conf.getTimeDuration(
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
      readLockHeldTimeStampNanos.set(timer.monotonicNowNanos());
    }
  }

  public void readUnlock() {
    readUnlock(OP_NAME_OTHER);
  }

  public void readUnlock(String opName) {
    final boolean needReport = coarseLock.getReadHoldCount() == 1;
    final long readLockIntervalNanos =
        timer.monotonicNowNanos() - readLockHeldTimeStampNanos.get();
    coarseLock.readLock().unlock();

    if (needReport) {
      addMetric(opName, readLockIntervalNanos, false);
      readLockHeldTimeStampNanos.remove();
    }
    final long readLockIntervalMs =
        TimeUnit.NANOSECONDS.toMillis(readLockIntervalNanos);
    if (needReport && readLockIntervalMs >= this.readLockReportingThresholdMs) {
      long localLongestReadLock;
      do {
        localLongestReadLock = longestReadLockHeldIntervalMs.get();
      } while (localLongestReadLock - readLockIntervalMs < 0 &&
          !longestReadLockHeldIntervalMs.compareAndSet(localLongestReadLock,
              readLockIntervalMs));

      long localTimeStampOfLastReadLockReport;
      long nowMs;
      do {
        nowMs = timer.monotonicNow();
        localTimeStampOfLastReadLockReport =
            timeStampOfLastReadLockReportMs.get();
        if (nowMs - localTimeStampOfLastReadLockReport <
            lockSuppressWarningIntervalMs) {
          numReadLockWarningsSuppressed.incrementAndGet();
          return;
        }
      } while (!timeStampOfLastReadLockReportMs.compareAndSet(
          localTimeStampOfLastReadLockReport, nowMs));
      int numSuppressedWarnings = numReadLockWarningsSuppressed.getAndSet(0);
      long longestLockHeldIntervalMs =
          longestReadLockHeldIntervalMs.getAndSet(0);
      FSNamesystem.LOG.info("FSNamesystem read lock held for " +
          readLockIntervalMs + " ms via\n" +
          StringUtils.getStackTrace(Thread.currentThread()) +
          "\tNumber of suppressed read-lock reports: " + numSuppressedWarnings +
          "\n\tLongest read-lock held interval: " + longestLockHeldIntervalMs);
    }
  }
  
  public void writeLock() {
    coarseLock.writeLock().lock();
    if (coarseLock.getWriteHoldCount() == 1) {
      writeLockHeldTimeStampNanos = timer.monotonicNowNanos();
    }
  }

  public void writeLockInterruptibly() throws InterruptedException {
    coarseLock.writeLock().lockInterruptibly();
    if (coarseLock.getWriteHoldCount() == 1) {
      writeLockHeldTimeStampNanos = timer.monotonicNowNanos();
    }
  }

  public void writeUnlock() {
    writeUnlock(OP_NAME_OTHER);
  }

  public void writeUnlock(String opName) {
    final boolean needReport = coarseLock.getWriteHoldCount() == 1 &&
        coarseLock.isWriteLockedByCurrentThread();
    final long currentTimeNanos = timer.monotonicNowNanos();
    final long writeLockIntervalNanos =
        currentTimeNanos - writeLockHeldTimeStampNanos;
    final long currentTimeMs = TimeUnit.NANOSECONDS.toMillis(currentTimeNanos);
    final long writeLockIntervalMs =
        TimeUnit.NANOSECONDS.toMillis(writeLockIntervalNanos);

    boolean logReport = false;
    int numSuppressedWarnings = 0;
    long longestLockHeldIntervalMs = 0;
    if (needReport &&
        writeLockIntervalMs >= this.writeLockReportingThresholdMs) {
      if (writeLockIntervalMs > longestWriteLockHeldIntervalMs) {
        longestWriteLockHeldIntervalMs = writeLockIntervalMs;
      }
      if (currentTimeMs - timeStampOfLastWriteLockReportMs >
          this.lockSuppressWarningIntervalMs) {
        logReport = true;
        numSuppressedWarnings = numWriteLockWarningsSuppressed;
        numWriteLockWarningsSuppressed = 0;
        longestLockHeldIntervalMs = longestWriteLockHeldIntervalMs;
        longestWriteLockHeldIntervalMs = 0;
        timeStampOfLastWriteLockReportMs = currentTimeMs;
      } else {
        numWriteLockWarningsSuppressed++;
      }
    }

    coarseLock.writeLock().unlock();

    if (needReport) {
      addMetric(opName, writeLockIntervalNanos, true);
    }

    if (logReport) {
      FSNamesystem.LOG.info("FSNamesystem write lock held for " +
          writeLockIntervalMs + " ms via\n" +
          StringUtils.getStackTrace(Thread.currentThread()) +
          "\tNumber of suppressed write-lock reports: " +
          numSuppressedWarnings + "\n\tLongest write-lock held interval: " +
          longestLockHeldIntervalMs);
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
   * @param value Length of time the lock was held (nanoseconds)
   */
  private void addMetric(String operationName, long value, boolean isWrite) {
    if (metricsEnabled) {
      String metricName =
          (isWrite ? WRITE_LOCK_METRIC_PREFIX : READ_LOCK_METRIC_PREFIX) +
          org.apache.commons.lang.StringUtils.capitalize(operationName) +
          LOCK_METRIC_SUFFIX;
      detailedHoldTimeMetrics.add(metricName, value);
    }
  }

}
