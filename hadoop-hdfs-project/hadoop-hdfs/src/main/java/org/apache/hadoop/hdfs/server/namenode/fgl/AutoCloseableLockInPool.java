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
package org.apache.hadoop.hdfs.server.namenode.fgl;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.io.Closeable;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.hdfs.server.namenode.fgl.LockPoolManager.LockMode;

/**
 * A {@link Closeable} lock that can be used with the try-with-resources construct.
 *
 * <pre>
 *   try (AutoCloseLockFromPool<K> lock = lockManager.acquireLock(key, mode) {
 *     ...
 *   }
 * </pre>
 *
 * Should not be manually constructed and should be managed by a {@link LockPoolManager} (or
 * its wrappers) instead.
 * @param <K> lock key
 */
public class AutoCloseableLockInPool<K> implements Closeable {

  /** The {@link LockPoolManager} that handles this lock. */
  private final LockPoolManager<K> lockManager;
  /** The underlying {@link LockResource} used by this class. */
  private final LockResource<K> lockResource;
  private LockMode lockMode;
  /**
   * The actual lock.
   * From {@link LockResource#rwLock} depending on {@link AutoCloseableLockInPool#lockMode}.
   */
  private Lock currentLock;

  public AutoCloseableLockInPool(LockPoolManager<K> manager, LockResource<K> lock, LockMode mode) {
    this.lockManager = manager;
    this.lockResource = lock;
    this.lockMode = mode;

    if (mode == LockMode.WRITE && getReadHoldCount() > 0) {
      // Release the lock here since it's already created by LockPoolManager.acquireLockResource
      this.lockManager.releaseLockResource(lockResource.getLockKey());
      throw new IllegalStateException(
          "Attempts to acquire the write lock while holding the read lock.");
    }

    if (this.lockMode == LockMode.READ) {
      this.currentLock = this.lockResource.getRwLock().readLock();
    } else {
      this.currentLock = this.lockResource.getRwLock().writeLock();
    }

    this.currentLock.lock();
  }

  @VisibleForTesting
  int getReadHoldCount() {
    return this.lockResource.getRwLock().getReadHoldCount();
  }

  public boolean hasReadLock() {
    return this.lockResource.getRwLock().getReadHoldCount() > 0 || hasWriteLock();
  }

  public boolean hasWriteLock() {
    return this.lockResource.getRwLock().isWriteLockedByCurrentThread();
  }

  /**
   * Swaps the lock mode from READ to WRITE.
   */
  public synchronized void changeToWriteMode() {
    // If currentLock == lockResource.readLock,
    // that means the current thread is holding the read lock.
    if (this.currentLock != this.lockResource.getRwLock().readLock()) {
      throw new IllegalStateException("The current thread must be holding the read lock.");
    }
    assert this.lockResource.getRwLock().getReadHoldCount() >= 1;
    this.currentLock.unlock();
    this.lockResource.getRwLock().writeLock().lock();
    this.currentLock = this.lockResource.getRwLock().writeLock();
    this.lockMode = LockMode.WRITE;
  }

  /**
   * Swaps the lock mode from WRITE to READ.
   */
  public synchronized void changeToReadMode() {
    // If currentLock == lockResource.writeLock,
    // that means the current thread is holding the write lock.
    if (this.currentLock != this.lockResource.getRwLock().writeLock()) {
      throw new IllegalStateException("The current thread must hold the write lock");
    }
    assert hasWriteLock();
    // Current thread can grab the read lock first before unlocking the write lock
    this.lockResource.getRwLock().readLock().lock();
    this.currentLock.unlock();
    this.currentLock = this.lockResource.getRwLock().readLock();
    this.lockMode = LockMode.READ;
  }

  @Override
  public synchronized void close() {
    if (currentLock == null) {
      throw new IllegalStateException("The lock is already closed.");
    }
    currentLock.unlock();
    currentLock = null;
    lockManager.releaseLockResourceAndRemoveFromLockTrace(lockResource);
  }

  @Override
  public synchronized String toString() {
    return "AutoCloseableLockInPool[" + this.lockResource + ", " + this.lockMode + "]";
  }
}
