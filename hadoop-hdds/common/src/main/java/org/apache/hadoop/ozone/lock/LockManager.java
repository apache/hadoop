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

/**
 * Manages the locks on a given resource. A new lock is created for each
 * and every unique resource. Uniqueness of resource depends on the
 * {@code equals} implementation of it.
 */
public class LockManager<T> {

  private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);

  private final Map<T, ActiveLock> activeLocks = new ConcurrentHashMap<>();
  private final GenericObjectPool<ActiveLock> lockPool =
      new GenericObjectPool<>(new PooledLockFactory());

  /**
   * Creates new LockManager instance.
   *
   * @param conf Configuration object
   */
  public LockManager(Configuration conf) {
    int maxPoolSize = conf.getInt(HddsConfigKeys.HDDS_LOCK_MAX_CONCURRENCY,
        HddsConfigKeys.HDDS_LOCK_MAX_CONCURRENCY_DEFAULT);
    lockPool.setMaxTotal(maxPoolSize);
  }


  /**
   * Acquires the lock on given resource.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the
   * lock has been acquired.
   */
  public void lock(T resource) {
    activeLocks.compute(resource, (k, v) -> {
      ActiveLock lock;
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
    }).lock();
  }

  /**
   * Releases the lock on given resource.
   */
  public void unlock(T resource) {
    ActiveLock lock = activeLocks.get(resource);
    if (lock == null) {
      // Someone is releasing a lock which was never acquired. Log and return.
      LOG.error("Trying to release the lock on {}, which was never acquired.",
          resource);
      throw new IllegalMonitorStateException("Releasing lock on resource "
          + resource + " without acquiring lock");
    }
    lock.unlock();
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