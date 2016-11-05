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
package org.apache.hadoop.util;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

/**
 * This is a wrap class of a <tt>ReadLock</tt>.
 * It extends the class {@link InstrumentedLock}, and can be used to track
 * whether a specific read lock is being held for too long and log
 * warnings if so.
 *
 * The logged warnings are throttled so that logs are not spammed.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InstrumentedReadLock extends InstrumentedLock {

  private final ReentrantReadWriteLock readWriteLock;

  /**
   * Uses the ThreadLocal to keep the time of acquiring locks since
   * there can be multiple threads that hold the read lock concurrently.
   */
  private final ThreadLocal<Long> readLockHeldTimeStamp =
      new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return Long.MAX_VALUE;
    };
  };

  public InstrumentedReadLock(String name, Log logger,
      ReentrantReadWriteLock readWriteLock,
      long minLoggingGapMs, long lockWarningThresholdMs) {
    this(name, logger, readWriteLock, minLoggingGapMs, lockWarningThresholdMs,
        new Timer());
  }

  @VisibleForTesting
  InstrumentedReadLock(String name, Log logger,
      ReentrantReadWriteLock readWriteLock,
      long minLoggingGapMs, long lockWarningThresholdMs, Timer clock) {
    super(name, logger, readWriteLock.readLock(), minLoggingGapMs,
        lockWarningThresholdMs, clock);
    this.readWriteLock = readWriteLock;
  }

  @Override
  public void unlock() {
    boolean needReport = readWriteLock.getReadHoldCount() == 1;
    long localLockReleaseTime = getTimer().monotonicNow();
    long localLockAcquireTime = readLockHeldTimeStamp.get();
    getLock().unlock();
    if (needReport) {
      readLockHeldTimeStamp.remove();
      check(localLockAcquireTime, localLockReleaseTime);
    }
  }

  /**
   * Starts timing for the instrumented read lock.
   * It records the time to ThreadLocal.
   */
  @Override
  protected void startLockTiming() {
    if (readWriteLock.getReadHoldCount() == 1) {
      readLockHeldTimeStamp.set(getTimer().monotonicNow());
    }
  }
}
