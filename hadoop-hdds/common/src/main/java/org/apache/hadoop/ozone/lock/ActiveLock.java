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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lock implementation which also maintains counter.
 */
public final class ActiveLock {

  private Lock lock;
  private AtomicInteger count;

  /**
   * Use ActiveLock#newInstance to create instance.
   */
  private ActiveLock() {
    this.lock = new ReentrantLock();
    this.count = new AtomicInteger(0);
  }

  /**
   * Creates a new instance of ActiveLock.
   *
   * @return new ActiveLock
   */
  public static ActiveLock newInstance() {
    return new ActiveLock();
  }

  /**
   * Acquires the lock.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the
   * lock has been acquired.
   */
  public void lock() {
    lock.lock();
  }

  /**
   * Releases the lock.
   */
  public void unlock() {
    lock.unlock();
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
