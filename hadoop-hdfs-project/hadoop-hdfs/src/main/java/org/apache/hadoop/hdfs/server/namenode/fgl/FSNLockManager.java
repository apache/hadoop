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
package org.apache.hadoop.hdfs.server.namenode.fgl;

import org.apache.hadoop.classification.VisibleForTesting;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.hadoop.hdfs.util.RwLockMode;

public interface FSNLockManager {

  /**
   * Acquire read lock for an operation according to the lock mode.
   * @param lockMode locking mode
   */
  void readLock(RwLockMode lockMode);

  /**
   * Acquire read lock according to the lock mode, unless interrupted while waiting.
   * @param lockMode locking mode
   * @throws InterruptedException If the thread is interrupted, an InterruptedException is thrown.
   */
  void readLockInterruptibly(RwLockMode lockMode) throws InterruptedException;

  /**
   * Release read lock for the operation according to the lock mode.
   * @param lockMode locking mode
   * @param opName operation name
   */
  void readUnlock(RwLockMode lockMode, String opName);

  /**
   * Release read lock for the operation according to the lock mode.
   * @param lockMode locking mode
   * @param opName operation name
   * @param lockReportInfoSupplier supplier used to report some information for this lock.
   */
  void readUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier);

  /**
   * Acquire write lock for an operation according to the lock mode.
   * @param lockMode locking mode
   */
  void writeLock(RwLockMode lockMode);

  /**
   * Release write lock for the operation according to the lock mode.
   * @param lockMode locking mode
   * @param opName operation name
   */
  void writeUnlock(RwLockMode lockMode, String opName);

  /**
   * Release write lock for the operation according to the lock mode.
   * @param lockMode locking mode
   * @param opName operation name
   * @param suppressWriteLockReport When false, event of write lock being held
   * for long time will be logged in logs and metrics.
   */
  void writeUnlock(RwLockMode lockMode, String opName,
      boolean suppressWriteLockReport);

  /**
   * Release write lock for the operation according to the lock mode.
   * @param lockMode locking mode
   * @param opName operation name
   * @param lockReportInfoSupplier supplier used to report information for this lock.
   */
  void writeUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier);

  void writeLockInterruptibly(RwLockMode lockMode) throws InterruptedException;

  /**
   * Check if the current thread holds write lock according to the lock mode.
   * @param lockMode locking mode
   * @return true if the current thread is holding the write-lock, else false.
   */
  boolean hasWriteLock(RwLockMode lockMode);

  /**
   * Check if the current thread holds read lock according to the lock mode.
   * @param lockMode locking mode
   * @return true if the current thread is holding the read-lock, else false.
   */
  boolean hasReadLock(RwLockMode lockMode);

  /**
   * Queries the number of reentrant read holds on this lock by the
   * current thread.  A reader thread has a hold on a lock for
   * each lock action that is not matched by an unlock action.
   *
   * @param lockMode locking mode
   * @return the number of holds on the read lock by the current thread,
   *         or zero if the read lock is not held by the current thread
   */
  int getReadHoldCount(RwLockMode lockMode);

  /**
   * Returns the QueueLength of waiting threads.
   * A larger number indicates greater lock contention.
   *
   * @param lockMode locking mode
   * @return int - Number of threads waiting on this lock
   */
  int getQueueLength(RwLockMode lockMode);

  /**
   * Returns the number of time the read lock
   * has been held longer than the threshold.
   *
   * @param lockMode locking mode
   * @return long - Number of time the read lock
   * has been held longer than the threshold
   */
  long getNumOfReadLockLongHold(RwLockMode lockMode);

  /**
   * Returns the number of time the write-lock
   * has been held longer than the threshold.
   *
   * @param lockMode locking mode
   * @return long - Number of time the write-lock
   * has been held longer than the threshold.
   */
  long getNumOfWriteLockLongHold(RwLockMode lockMode);

  /**
   * Check if the metrics is enabled.
   * @return true if the metrics is enabled, else false.
   */
  boolean isMetricsEnabled();

  /**
   * Reset the metricsEnabled according to the lock mode.
   * @param metricsEnabled the new metricsEnabled
   */
  void setMetricsEnabled(boolean metricsEnabled);

  /**
   * Try to set the reporting threshold of the read lock.
   * @param readLockReportingThresholdMs reporting threshold
   */
  void setReadLockReportingThresholdMs(long readLockReportingThresholdMs);

  /**
   * Try to get the reporting threshold of the read lock.
   * @return the reporting threshold.
   */
  long getReadLockReportingThresholdMs();

  /**
   * Try to set the reporting threshold for the write lock.
   * @param writeLockReportingThresholdMs reporting threshold.
   */
  void setWriteLockReportingThresholdMs(long writeLockReportingThresholdMs);

  /**
   * Try to get the reporting threshold for the write lock.
   * @return reporting threshold.
   */
  long getWriteLockReportingThresholdMs();

  @VisibleForTesting
  void setLockForTests(ReentrantReadWriteLock lock);

  @VisibleForTesting
  ReentrantReadWriteLock getLockForTests();
}
