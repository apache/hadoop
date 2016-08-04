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

import java.util.concurrent.locks.ReentrantLock;

/**
 * This is a wrap class of a ReentrantLock. Extending AutoCloseable
 * interface such that the users can use a try-with-resource syntax.
 */
public class AutoCloseableLock implements AutoCloseable {

  private final ReentrantLock lock;

  /**
   * Creates an instance of {@code AutoCloseableLock}, initializes
   * the underlying {@code ReentrantLock} object.
   */
  public AutoCloseableLock() {
    this.lock = new ReentrantLock();
  }

  /**
   * A wrapper method that makes a call to {@code lock()} of the underlying
   * {@code ReentrantLock} object.
   *
   * Acquire teh lock it is not held by another thread, then sets
   * lock held count to one, then returns immediately.
   *
   * If the current thread already holds the lock, increase the lock
   * help count by one and returns immediately.
   *
   * If the lock is held by another thread, the current thread is
   * suspended until the lock has been acquired by current thread.
   *
   * @return The {@code ReentrantLock} object itself. This is to
   * support try-with-resource syntax.
   */
  public AutoCloseableLock acquire() {
    lock.lock();
    return this;
  }

  /**
   * A wrapper method that makes a call to {@code unlock()} of the
   * underlying {@code ReentrantLock} object.
   *
   * Attempts to release the lock.
   *
   * If the current thread holds the lock, decrements the hold
   * count. If the hold count reaches zero, the lock is released.
   *
   * If the current thread does not hold the lock, then
   * {@link IllegalMonitorStateException} is thrown.
   */
  public void release() {
    lock.unlock();
  }

  /**
   * Attempts to release the lock by making a call to {@code release()}.
   *
   * This is to implement {@code close()} method from {@code AutoCloseable}
   * interface. This allows users to user a try-with-resource syntax, where
   * the lock can be automatically released.
   */
  @Override
  public void close() {
    release();
  }

  /**
   * A wrapper method that makes a call to {@code tryLock()} of
   * the underlying {@code ReentrantLock} object.
   *
   * If the lock is not held by another thread, acquires the lock, set the
   * hold count to one and returns {@code true}.
   *
   * If the current thread already holds the lock, the increment the hold
   * count by one and returns {@code true}.
   *
   * If the lock is held by another thread then the method returns
   * immediately with {@code false}.
   *
   * @return {@code true} if the lock was free and was acquired by the
   *          current thread, or the lock was already held by the current
   *          thread; and {@code false} otherwise.
   */
  public boolean tryLock() {
    return lock.tryLock();
  }

  /**
   * A wrapper method that makes a call to {@code isLocked()} of
   * the underlying {@code ReentrantLock} object.
   *
   * Queries if this lock is held by any thread. This method is
   * designed for use in monitoring of the system state,
   * not for synchronization control.
   *
   * @return {@code true} if any thread holds this lock and
   *         {@code false} otherwise
   */
  public boolean isLocked() {
    return lock.isLocked();
  }
}
