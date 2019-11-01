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

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Manages the locks on a given resource. A new lock is created for each
 * and every unique resource. Uniqueness of resource depends on the
 * {@code equals} implementation of it.
 */
public class LockManager<R> {

  private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);

  private final Map<R, ActiveLock> activeLocks = new ConcurrentHashMap<>();
  private final GenericObjectPool<ActiveLock> lockPool;

  /**
   * Creates new LockManager instance with the given Configuration.and uses
   * non-fair mode for locks.
   *
   * @param conf Configuration object
   */
  public LockManager(final Configuration conf) {
    this(conf, false);
  }


  /**
   * Creates new LockManager instance with the given Configuration.
   *
   * @param conf Configuration object
   * @param fair - true to use fair lock ordering, else non-fair lock ordering.
   */
  public LockManager(final Configuration conf, boolean fair) {
    final int maxPoolSize = conf.getInt(
        HddsConfigKeys.HDDS_LOCK_MAX_CONCURRENCY,
        HddsConfigKeys.HDDS_LOCK_MAX_CONCURRENCY_DEFAULT);
    lockPool =
        new GenericObjectPool<>(new PooledLockFactory(fair));
    lockPool.setMaxTotal(maxPoolSize);
  }

  /**
   * Acquires the lock on given resource.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the
   * lock has been acquired.
   *
   * @param resource on which the lock has to be acquired
   * @deprecated Use {@link LockManager#writeLock} instead
   */
  public void lock(final R resource) {
    writeLock(resource);
  }

  /**
   * Releases the lock on given resource.
   *
   * @param resource for which the lock has to be released
   * @deprecated Use {@link LockManager#writeUnlock} instead
   */
  public void unlock(final R resource) {
    writeUnlock(resource);
  }

  /**
   * Acquires the read lock on given resource.
   *
   * <p>Acquires the read lock on resource if the write lock is not held by
   * another thread and returns immediately.
   *
   * <p>If the write lock on resource is held by another thread then
   * the current thread becomes disabled for thread scheduling
   * purposes and lies dormant until the read lock has been acquired.
   *
   * @param resource on which the read lock has to be acquired
   */
  public void readLock(final R resource) {
    acquire(resource, ActiveLock::readLock);
  }

  /**
   * Releases the read lock on given resource.
   *
   * @param resource for which the read lock has to be released
   * @throws IllegalMonitorStateException if the current thread does not
   *                                      hold this lock
   */
  public void readUnlock(final R resource) throws IllegalMonitorStateException {
    release(resource, ActiveLock::readUnlock);
  }

  /**
   * Acquires the write lock on given resource.
   *
   * <p>Acquires the write lock on resource if neither the read nor write lock
   * are held by another thread and returns immediately.
   *
   * <p>If the current thread already holds the write lock then the
   * hold count is incremented by one and the method returns
   * immediately.
   *
   * <p>If the lock is held by another thread then the current
   * thread becomes disabled for thread scheduling purposes and
   * lies dormant until the write lock has been acquired.
   *
   * @param resource on which the lock has to be acquired
   */
  public void writeLock(final R resource) {
    acquire(resource, ActiveLock::writeLock);
  }

  /**
   * Releases the write lock on given resource.
   *
   * @param resource for which the lock has to be released
   * @throws IllegalMonitorStateException if the current thread does not
   *                                      hold this lock
   */
  public void writeUnlock(final R resource)
      throws IllegalMonitorStateException {
    release(resource, ActiveLock::writeUnlock);
  }

  /**
   * Acquires the lock on given resource using the provided lock function.
   *
   * @param resource on which the lock has to be acquired
   * @param lockFn function to acquire the lock
   */
  private void acquire(final R resource, final Consumer<ActiveLock> lockFn) {
    lockFn.accept(getLockForLocking(resource));
  }

  /**
   * Releases the lock on given resource using the provided release function.
   *
   * @param resource for which the lock has to be released
   * @param releaseFn function to release the lock
   */
  private void release(final R resource, final Consumer<ActiveLock> releaseFn) {
    final ActiveLock lock = getLockForReleasing(resource);
    releaseFn.accept(lock);
    decrementActiveLockCount(resource);
  }

  /**
   * Returns {@link ActiveLock} instance for the given resource,
   * on which the lock can be acquired.
   *
   * @param resource on which the lock has to be acquired
   * @return {@link ActiveLock} instance
   */
  private ActiveLock getLockForLocking(final R resource) {
    /*
     * While getting a lock object for locking we should
     * atomically increment the active count of the lock.
     *
     * This is to avoid cases where the selected lock could
     * be removed from the activeLocks map and returned to
     * the object pool.
     */
    return activeLocks.compute(resource, (k, v) -> {
      final ActiveLock lock;
      try {
        if (v == null) {
          lock = lockPool.borrowObject();
        } else {
          lock = v;
        }
        lock.incrementActiveCount();
      } catch (Exception ex) {
        LOG.error("Unable to obtain lock.", ex);
        throw new RuntimeException(ex);
      }
      return lock;
    });
  }

  /**
   * Returns {@link ActiveLock} instance for the given resource,
   * for which the lock has to be released.
   *
   * @param resource for which the lock has to be released
   * @return {@link ActiveLock} instance
   */
  private ActiveLock getLockForReleasing(final R resource) {
    if (activeLocks.containsKey(resource)) {
      return activeLocks.get(resource);
    }
    // Someone is releasing a lock which was never acquired.
    LOG.error("Trying to release the lock on {}, which was never acquired.",
        resource);
    throw new IllegalMonitorStateException("Releasing lock on resource "
        + resource + " without acquiring lock");
  }

  /**
   * Decrements the active lock count and returns the {@link ActiveLock}
   * object to pool if the active count is 0.
   *
   * @param resource resource to which the ActiveLock is associated
   */
  private void decrementActiveLockCount(final R resource) {
    activeLocks.computeIfPresent(resource, (k, v) -> {
      v.decrementActiveCount();
      if (v.getActiveLockCount() != 0) {
        return v;
      }
      lockPool.returnObject(v);
      return null;
    });
  }

}