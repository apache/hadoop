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
import org.apache.commons.logging.Log;

import com.google.common.annotations.VisibleForTesting;

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
  private final Log logger;
  private final String name;
  private final Timer clock;

  /** Minimum gap between two lock warnings. */
  private final long minLoggingGap;
  /** Threshold for detecting long lock held time. */
  private final long lockWarningThreshold;

  // Tracking counters for lock statistics.
  private volatile long lockAcquireTimestamp;
  private final AtomicLong lastLogTimestamp;
  private final AtomicLong warningsSuppressed = new AtomicLong(0);

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
  public InstrumentedLock(String name, Log logger, long minLoggingGapMs,
      long lockWarningThresholdMs) {
    this(name, logger, new ReentrantLock(),
        minLoggingGapMs, lockWarningThresholdMs);
  }

  public InstrumentedLock(String name, Log logger, Lock lock,
      long minLoggingGapMs, long lockWarningThresholdMs) {
    this(name, logger, lock,
        minLoggingGapMs, lockWarningThresholdMs, new Timer());
  }

  @VisibleForTesting
  InstrumentedLock(String name, Log logger, Lock lock,
      long minLoggingGapMs, long lockWarningThresholdMs, Timer clock) {
    this.name = name;
    this.lock = lock;
    this.clock = clock;
    this.logger = logger;
    minLoggingGap = minLoggingGapMs;
    lockWarningThreshold = lockWarningThresholdMs;
    lastLogTimestamp = new AtomicLong(
      clock.monotonicNow() - Math.max(minLoggingGap, lockWarningThreshold));
  }

  @Override
  public void lock() {
    lock.lock();
    startLockTiming();
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    lock.lockInterruptibly();
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
    if (lock.tryLock(time, unit)) {
      startLockTiming();
      return true;
    }
    return false;
  }

  @Override
  public void unlock() {
    long localLockReleaseTime = clock.monotonicNow();
    long localLockAcquireTime = lockAcquireTimestamp;
    lock.unlock();
    check(localLockAcquireTime, localLockReleaseTime);
  }

  @Override
  public Condition newCondition() {
    return lock.newCondition();
  }

  @VisibleForTesting
  void logWarning(long lockHeldTime, long suppressed) {
    logger.warn(String.format("Lock held time above threshold: " +
        "lock identifier: %s " +
        "lockHeldTimeMs=%d ms. Suppressed %d lock warnings. " +
        "The stack trace is: %s" ,
        name, lockHeldTime, suppressed,
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
  protected void check(long acquireTime, long releaseTime) {
    if (!logger.isWarnEnabled()) {
      return;
    }

    final long lockHeldTime = releaseTime - acquireTime;
    if (lockWarningThreshold - lockHeldTime < 0) {
      long now;
      long localLastLogTs;
      do {
        now = clock.monotonicNow();
        localLastLogTs = lastLogTimestamp.get();
        long deltaSinceLastLog = now - localLastLogTs;
        // check should print log or not
        if (deltaSinceLastLog - minLoggingGap < 0) {
          warningsSuppressed.incrementAndGet();
          return;
        }
      } while (!lastLogTimestamp.compareAndSet(localLastLogTs, now));
      long suppressed = warningsSuppressed.getAndSet(0);
      logWarning(lockHeldTime, suppressed);
    }
  }

  protected Lock getLock() {
    return lock;
  }

  protected Timer getTimer() {
    return clock;
  }
}
