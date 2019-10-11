/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.lock;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Lock implementation which also maintains counter.
 */
public final class ActiveLock {

  private ReadWriteLock lock;
  private AtomicInteger count;

  /**
   * Use ActiveLock#newInstance to create instance.
   *
   * @param fairness - if true the lock uses a fair ordering policy, else
   * non-fair ordering.
   */
  private ActiveLock(boolean fairness) {
    this.lock = new ReentrantReadWriteLock(fairness);
    this.count = new AtomicInteger(0);
  }

  /**
   * Creates a new instance of ActiveLock.
   *
   * @return new ActiveLock
   */
  public static ActiveLock newInstance(boolean fairness) {
    return new ActiveLock(fairness);
  }

  /**
   * Acquires read lock.
   *
   * <p>Acquires the read lock if the write lock is not held by
   * another thread and returns immediately.
   *
   * <p>If the write lock is held by another thread then
   * the current thread becomes disabled for thread scheduling
   * purposes and lies dormant until the read lock has been acquired.
   */
  void readLock() {
    lock.readLock().lock();
  }

  /**
   * Attempts to release the read lock.
   *
   * <p>If the number of readers is now zero then the lock
   * is made available for write lock attempts.
   */
  void readUnlock() {
    lock.readLock().unlock();
  }

  /**
   * Acquires write lock.
   *
   * <p>Acquires the write lock if neither the read nor write lock
   * are held by another thread
   * and returns immediately, setting the write lock hold count to
   * one.
   *
   * <p>If the current thread already holds the write lock then the
   * hold count is incremented by one and the method returns
   * immediately.
   *
   * <p>If the lock is held by another thread then the current
   * thread becomes disabled for thread scheduling purposes and
   * lies dormant until the write lock has been acquired.
   */
  void writeLock() {
    lock.writeLock().lock();
  }

  /**
   * Attempts to release the write lock.
   *
   * <p>If the current thread is the holder of this lock then
   * the hold count is decremented. If the hold count is now
   * zero then the lock is released.
   */
  void writeUnlock() {
    lock.writeLock().unlock();
  }

  /**
   * Increment the active count of the lock.
   */
  void incrementActiveCount() {
    count.incrementAndGet();
  }

  /**
   * Decrement the active count of the lock.
   */
  void decrementActiveCount() {
    count.decrementAndGet();
  }

  /**
   * Returns the active count on the lock.
   *
   * @return Number of active leases on the lock.
   */
  int getActiveLockCount() {
    return count.get();
  }

  /**
   * Resets the active count on the lock.
   */
  void resetCounter() {
    count.set(0);
  }

  @Override
  public String toString() {
    return lock.toString();
  }
}
