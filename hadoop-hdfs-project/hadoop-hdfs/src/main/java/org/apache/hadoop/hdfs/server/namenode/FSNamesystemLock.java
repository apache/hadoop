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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.log.LogThrottlingHelper;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
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
import static org.apache.hadoop.ipc.ProcessingDetails.Timing;
import static org.apache.hadoop.log.LogThrottlingHelper.LogAction;

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
  /** Frequency limiter used for reporting long write lock hold times. */
  private final LogThrottlingHelper writeLockReportLogger;

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
  /**
   * The info (lock held time and stack trace) when longest time (ms) a read
   * lock was held since the last report.
   */
  private final AtomicReference<LockHeldInfo> longestReadLockHeldInfo =
      new AtomicReference<>(new LockHeldInfo(0, 0, null));
  private LockHeldInfo longestWriteLockHeldInfo = new LockHeldInfo(0, 0, null);
  /**
   * The number of time the read lock
   * has been held longer than the threshold.
   */
  private final LongAdder numReadLockLongHold = new LongAdder();
  /**
   * The number of time the write lock
   * has been held for longer than the threshold.
   */
  private final LongAdder numWriteLockLongHold = new LongAdder();

  @VisibleForTesting
  static final String OP_NAME_OTHER = "OTHER";
  private static final String READ_LOCK_METRIC_PREFIX = "FSNReadLock";
  private static final String WRITE_LOCK_METRIC_PREFIX = "FSNWriteLock";
  private static final String LOCK_METRIC_SUFFIX = "Nanos";

  private static final String OVERALL_METRIC_NAME = "Overall";

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
    this.writeLockReportLogger =
        new LogThrottlingHelper(lockSuppressWarningIntervalMs);
    this.metricsEnabled = conf.getBoolean(
        DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY,
        DFS_NAMENODE_LOCK_DETAILED_METRICS_DEFAULT);
    FSNamesystem.LOG.info("Detailed lock hold time metrics enabled: " +
        this.metricsEnabled);
    this.detailedHoldTimeMetrics = detailedHoldTimeMetrics;
  }

  public void readLock() {
    doLock(false);
  }

  public void readLockInterruptibly() throws InterruptedException {
    doLockInterruptibly(false);
  }

  public void readUnlock() {
    readUnlock(OP_NAME_OTHER);
  }

  public void readUnlock(String opName) {
    final boolean needReport = coarseLock.getReadHoldCount() == 1;
    final long readLockIntervalNanos =
        timer.monotonicNowNanos() - readLockHeldTimeStampNanos.get();
    final long currentTimeMs = timer.now();
    coarseLock.readLock().unlock();

    if (needReport) {
      addMetric(opName, readLockIntervalNanos, false);
      readLockHeldTimeStampNanos.remove();
    }
    final long readLockIntervalMs =
        TimeUnit.NANOSECONDS.toMillis(readLockIntervalNanos);
    if (needReport && readLockIntervalMs >= this.readLockReportingThresholdMs) {
      numReadLockLongHold.increment();
      LockHeldInfo localLockHeldInfo;
      do {
        localLockHeldInfo = longestReadLockHeldInfo.get();
      } while (localLockHeldInfo.getIntervalMs() - readLockIntervalMs < 0 &&
          !longestReadLockHeldInfo.compareAndSet(localLockHeldInfo,
              new LockHeldInfo(currentTimeMs, readLockIntervalMs,
                  StringUtils.getStackTrace(Thread.currentThread()))));

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
      LockHeldInfo lockHeldInfo =
          longestReadLockHeldInfo.getAndSet(new LockHeldInfo(0, 0, null));
      FSNamesystem.LOG.info(
          "\tNumber of suppressed read-lock reports: {}"
              + "\n\tLongest read-lock held at {} for {}ms via {}",
          numSuppressedWarnings, Time.formatTime(lockHeldInfo.getStartTimeMs()),
          lockHeldInfo.getIntervalMs(), lockHeldInfo.getStackTrace());
    }
  }
  
  public void writeLock() {
    doLock(true);
  }

  public void writeLockInterruptibly() throws InterruptedException {
    doLockInterruptibly(true);
  }

  /**
   * Unlocks FSNameSystem write lock. This internally calls {@link
   * FSNamesystemLock#writeUnlock(String, boolean)}
   */
  public void writeUnlock() {
    writeUnlock(OP_NAME_OTHER, false);
  }

  /**
   * Unlocks FSNameSystem write lock. This internally calls {@link
   * FSNamesystemLock#writeUnlock(String, boolean)}
   *
   * @param opName Operation name.
   */
  public void writeUnlock(String opName) {
    writeUnlock(opName, false);
  }

  /**
   * Unlocks FSNameSystem write lock.
   *
   * @param opName Operation name
   * @param suppressWriteLockReport When false, event of write lock being held
   * for long time will be logged in logs and metrics.
   */
  public void writeUnlock(String opName, boolean suppressWriteLockReport) {
    final boolean needReport = !suppressWriteLockReport && coarseLock
        .getWriteHoldCount() == 1 && coarseLock.isWriteLockedByCurrentThread();
    final long writeLockIntervalNanos =
        timer.monotonicNowNanos() - writeLockHeldTimeStampNanos;
    final long currentTimeMs = timer.now();
    final long writeLockIntervalMs =
        TimeUnit.NANOSECONDS.toMillis(writeLockIntervalNanos);

    LogAction logAction = LogThrottlingHelper.DO_NOT_LOG;
    if (needReport &&
        writeLockIntervalMs >= this.writeLockReportingThresholdMs) {
      numWriteLockLongHold.increment();
      if (longestWriteLockHeldInfo.getIntervalMs() < writeLockIntervalMs) {
        longestWriteLockHeldInfo =
            new LockHeldInfo(currentTimeMs, writeLockIntervalMs,
                StringUtils.getStackTrace(Thread.currentThread()));
      }

      logAction = writeLockReportLogger
          .record("write", currentTimeMs, writeLockIntervalMs);
    }

    LockHeldInfo lockHeldInfo = longestWriteLockHeldInfo;
    if (logAction.shouldLog()) {
      longestWriteLockHeldInfo = new LockHeldInfo(0, 0, null);
    }

    coarseLock.writeLock().unlock();

    if (needReport) {
      addMetric(opName, writeLockIntervalNanos, true);
    }

    if (logAction.shouldLog()) {
      FSNamesystem.LOG.info(
          "\tNumber of suppressed write-lock reports: {}"
              + "\n\tLongest write-lock held at {} for {}ms via {}"
              + "\n\tTotal suppressed write-lock held time: {}",
          logAction.getCount() - 1,
          Time.formatTime(lockHeldInfo.getStartTimeMs()),
          lockHeldInfo.getIntervalMs(), lockHeldInfo.getStackTrace(),
          logAction.getStats(0).getSum() - lockHeldInfo.getIntervalMs());
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
   * Returns the number of time the read lock
   * has been held longer than the threshold.
   *
   * @return long - Number of time the read lock
   * has been held longer than the threshold
   */
  public long getNumOfReadLockLongHold() {
    return numReadLockLongHold.longValue();
  }

  /**
   * Returns the number of time the write lock
   * has been held longer than the threshold.
   *
   * @return long - Number of time the write lock
   * has been held longer than the threshold.
   */
  public long getNumOfWriteLockLongHold() {
    return numWriteLockLongHold.longValue();
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
      String opMetric = getMetricName(operationName, isWrite);
      detailedHoldTimeMetrics.add(opMetric, value);

      String overallMetric = getMetricName(OVERALL_METRIC_NAME, isWrite);
      detailedHoldTimeMetrics.add(overallMetric, value);
    }
    updateProcessingDetails(
        isWrite ? Timing.LOCKEXCLUSIVE : Timing.LOCKSHARED, value);
  }

  private void doLock(boolean isWrite) {
    long startNanos = timer.monotonicNowNanos();
    if (isWrite) {
      coarseLock.writeLock().lock();
    } else {
      coarseLock.readLock().lock();
    }
    updateLockWait(startNanos, isWrite);
  }

  private void doLockInterruptibly(boolean isWrite)
      throws InterruptedException {
    long startNanos = timer.monotonicNowNanos();
    if (isWrite) {
      coarseLock.writeLock().lockInterruptibly();
    } else {
      coarseLock.readLock().lockInterruptibly();
    }
    updateLockWait(startNanos, isWrite);
  }

  private void updateLockWait(long startNanos, boolean isWrite) {
    long now = timer.monotonicNowNanos();
    updateProcessingDetails(Timing.LOCKWAIT, now - startNanos);
    if (isWrite) {
      if (coarseLock.getWriteHoldCount() == 1) {
        writeLockHeldTimeStampNanos = now;
      }
    } else {
      if (coarseLock.getReadHoldCount() == 1) {
        readLockHeldTimeStampNanos.set(now);
      }
    }
  }

  private static void updateProcessingDetails(Timing type, long deltaNanos) {
    Server.Call call = Server.getCurCall().get();
    if (call != null) {
      call.getProcessingDetails().add(type, deltaNanos, TimeUnit.NANOSECONDS);
    }
  }

  private static String getMetricName(String operationName, boolean isWrite) {
    return (isWrite ? WRITE_LOCK_METRIC_PREFIX : READ_LOCK_METRIC_PREFIX) +
        org.apache.commons.lang3.StringUtils.capitalize(operationName) +
        LOCK_METRIC_SUFFIX;
  }

  /**
   * Read lock Held Info.
   */
  private static class LockHeldInfo {
    /** Lock held start time. */
    private Long startTimeMs;
    /** Lock held time. */
    private Long intervalMs;
    /** The stack trace lock was held. */
    private String stackTrace;

    LockHeldInfo(long startTimeMs, long intervalMs, String stackTrace) {
      this.startTimeMs = startTimeMs;
      this.intervalMs = intervalMs;
      this.stackTrace = stackTrace;
    }

    public Long getStartTimeMs() {
      return this.startTimeMs;
    }

    public Long getIntervalMs() {
      return this.intervalMs;
    }

    public String getStackTrace() {
      return this.stackTrace;
    }
  }
}
