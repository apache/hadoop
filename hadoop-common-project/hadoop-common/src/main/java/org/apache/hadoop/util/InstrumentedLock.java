/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;

/**
 * This is a debugging class that can be used by callers to track
 * whether a specific lock is being held for too long and periodically
 * log a warning and stack trace, if so.
 *
 * The logged warnings are throttled so that logs are not spammed.
 *
 * A new instance of InstrumentedLock can be created for each object
 * that needs to be instrumented.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InstrumentedLock implements Lock {

  private final Lock lock;
  private final Logger logger;
  private final String name;
  private final Timer clock;

  /** Minimum gap between two lock warnings. */
  private final long minLoggingGap;
  /** Threshold for detecting long lock held time. */
  private final long lockWarningThreshold;

  // Tracking counters for lock statistics.
  private volatile long lockAcquireTimestamp;
  private final AtomicLong lastHoldLogTimestamp;
  private final AtomicLong lastWaitLogTimestamp;
  private final SuppressedStats holdStats = new SuppressedStats();
  private final SuppressedStats waitStats = new SuppressedStats();

  /**
   * Create a instrumented lock instance which logs a warning message
   * when lock held time is above given threshold.
   *
   * @param name the identifier of the lock object
   * @param logger this class does not have its own logger, will log to the
   *               given logger instead
   * @param minLoggingGapMs  the minimum time gap between two log messages,
   *                         this is to avoid spamming to many logs
   * @param lockWarningThresholdMs the time threshold to view lock held
   *                               time as being "too long"
   */
  public InstrumentedLock(String name, Logger logger, long minLoggingGapMs,
                          long lockWarningThresholdMs) {
    this(name, logger, new ReentrantLock(),
        minLoggingGapMs, lockWarningThresholdMs);
  }

  public InstrumentedLock(String name, Logger logger, Lock lock,
      long minLoggingGapMs, long lockWarningThresholdMs) {
    this(name, logger, lock,
        minLoggingGapMs, lockWarningThresholdMs, new Timer());
  }

  @VisibleForTesting
  InstrumentedLock(String name, Logger logger, Lock lock,
      long minLoggingGapMs, long lockWarningThresholdMs, Timer clock) {
    this.name = name;
    this.lock = lock;
    this.clock = clock;
    this.logger = logger;
    minLoggingGap = minLoggingGapMs;
    lockWarningThreshold = lockWarningThresholdMs;
    lastHoldLogTimestamp = new AtomicLong(
      clock.monotonicNow() - Math.max(minLoggingGap, lockWarningThreshold));
    lastWaitLogTimestamp = new AtomicLong(lastHoldLogTimestamp.get());
  }

  @Override
  public void lock() {
    long waitStart = clock.monotonicNow();
    lock.lock();
    check(waitStart, clock.monotonicNow(), false);
    startLockTiming();
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    long waitStart = clock.monotonicNow();
    lock.lockInterruptibly();
    check(waitStart, clock.monotonicNow(), false);
    startLockTiming();
  }

  @Override
  public boolean tryLock() {
    if (lock.tryLock()) {
      startLockTiming();
      return true;
    }
    return false;
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    long waitStart = clock.monotonicNow();
    boolean retval = false;
    if (lock.tryLock(time, unit)) {
      startLockTiming();
      retval = true;
    }
    check(waitStart, clock.monotonicNow(), false);
    return retval;
  }

  @Override
  public void unlock() {
    long localLockReleaseTime = clock.monotonicNow();
    long localLockAcquireTime = lockAcquireTimestamp;
    lock.unlock();
    check(localLockAcquireTime, localLockReleaseTime, true);
  }

  @Override
  public Condition newCondition() {
    return lock.newCondition();
  }

  @VisibleForTesting
  void logWarning(long lockHeldTime, SuppressedSnapshot stats) {
    logger.warn(String.format("Lock held time above threshold: " +
        "lock identifier: %s " +
        "lockHeldTimeMs=%d ms. Suppressed %d lock warnings. " +
        "Longest suppressed LockHeldTimeMs=%d. " +
        "The stack trace is: %s" ,
        name, lockHeldTime, stats.getSuppressedCount(),
        stats.getMaxSuppressedWait(),
        StringUtils.getStackTrace(Thread.currentThread())));
  }

  @VisibleForTesting
  void logWaitWarning(long lockWaitTime, SuppressedSnapshot stats) {
    logger.warn(String.format("Waited above threshold to acquire lock: " +
        "lock identifier: %s " +
        "waitTimeMs=%d ms. Suppressed %d lock wait warnings. " +
        "Longest suppressed WaitTimeMs=%d. " +
        "The stack trace is: %s", name, lockWaitTime,
        stats.getSuppressedCount(), stats.getMaxSuppressedWait(),
        StringUtils.getStackTrace(Thread.currentThread())));
  }

  /**
   * Starts timing for the instrumented lock.
   */
  protected void startLockTiming() {
    lockAcquireTimestamp = clock.monotonicNow();
  }

  /**
   * Log a warning if the lock was held for too long.
   *
   * Should be invoked by the caller immediately AFTER releasing the lock.
   *
   * @param acquireTime  - timestamp just after acquiring the lock.
   * @param releaseTime - timestamp just before releasing the lock.
   */
  protected void check(long acquireTime, long releaseTime,
       boolean checkLockHeld) {
    if (!logger.isWarnEnabled()) {
      return;
    }

    final long lockHeldTime = releaseTime - acquireTime;
    if (lockWarningThreshold - lockHeldTime < 0) {
      AtomicLong lastLogTime;
      SuppressedStats stats;
      if (checkLockHeld) {
        lastLogTime = lastHoldLogTimestamp;
        stats = holdStats;
      } else {
        lastLogTime = lastWaitLogTimestamp;
        stats = waitStats;
      }
      long now;
      long localLastLogTs;
      do {
        now = clock.monotonicNow();
        localLastLogTs = lastLogTime.get();
        long deltaSinceLastLog = now - localLastLogTs;
        // check should print log or not
        if (deltaSinceLastLog - minLoggingGap < 0) {
          stats.incrementSuppressed(lockHeldTime);
          return;
        }
      } while (!lastLogTime.compareAndSet(localLastLogTs, now));
      SuppressedSnapshot statsSnapshot = stats.snapshot();
      if (checkLockHeld) {
        logWarning(lockHeldTime, statsSnapshot);
      } else {
        logWaitWarning(lockHeldTime, statsSnapshot);
      }
    }
  }

  protected Lock getLock() {
    return lock;
  }

  protected Timer getTimer() {
    return clock;
  }

  /**
   * Internal class to track statistics about suppressed log messages in an
   * atomic way.
   */
  private static class SuppressedStats {
    private long suppressedCount = 0;
    private long maxSuppressedWait = 0;

    /**
     * Increments the suppressed counter and increases the max wait time if the
     * passed wait is greater than the current maxSuppressedWait.
     * @param wait The wait time for this suppressed message
     */
    synchronized public void incrementSuppressed(long wait) {
      suppressedCount++;
      if (wait > maxSuppressedWait) {
        maxSuppressedWait = wait;
      }
    }

    /**
     * Captures the current value of the counts into a SuppressedSnapshot object
     * and resets the values to zero.
     *
     * @return SuppressedSnapshot containing the current value of the counters
     */
    synchronized public SuppressedSnapshot snapshot() {
      SuppressedSnapshot snap =
          new SuppressedSnapshot(suppressedCount, maxSuppressedWait);
      suppressedCount = 0;
      maxSuppressedWait = 0;
      return snap;
    }
  }

  /**
   * Immutable class to capture a snapshot of suppressed log message stats.
   */
  protected static class SuppressedSnapshot {
    private long suppressedCount = 0;
    private long maxSuppressedWait = 0;

    public SuppressedSnapshot(long suppressedCount, long maxWait) {
      this.suppressedCount = suppressedCount;
      this.maxSuppressedWait = maxWait;
    }

    public long getMaxSuppressedWait() {
      return maxSuppressedWait;
    }

    public long getSuppressedCount() {
      return suppressedCount;
    }
  }
}
