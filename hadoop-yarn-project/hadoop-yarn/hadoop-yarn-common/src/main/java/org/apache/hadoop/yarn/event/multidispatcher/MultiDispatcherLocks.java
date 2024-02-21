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

package org.apache.hadoop.yarn.event.multidispatcher;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;

import org.apache.hadoop.yarn.event.Event;

/**
 * The locking logic of the {@link MultiDispatcher}
 */
class MultiDispatcherLocks {
  private final Logger LOG;
  private final Set<String> lockKeys= ConcurrentHashMap.newKeySet();
  private final Lock globalLock = new ReentrantLock();

  public MultiDispatcherLocks(Logger LOG) {
    this.LOG = LOG;
  }

  void lock(Event event) {
    String lockKey = event.getLockKey();
    debugLockLog(lockKey, "lock", event);
    if (lockKey == null) {
      globalLock.lock();
    } else {
      lockKeys.add(lockKey);
    }
    debugLockLog(lockKey, "locked", event);
  }

  void unLock(Event event) {
    String lockKey = event.getLockKey();
    debugLockLog(lockKey, "unlock", event);
    if (lockKey == null) {
      globalLock.unlock();
    } else {
      lockKeys.remove(lockKey);
    }
    debugLockLog(lockKey, "unlocked", event);
  }

  private void debugLockLog(String lockKey, String command, Event event) {
    LOG.debug("{} {} with thread {} for event {}",
        lockKey, command, Thread.currentThread().getName(), event.getClass().getSimpleName());
  }
}
