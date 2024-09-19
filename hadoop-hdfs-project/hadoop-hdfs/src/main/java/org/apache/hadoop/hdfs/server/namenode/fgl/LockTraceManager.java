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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Stack;

/**
 * A class for checking deadlocks by tracing threads holding this lock.
 */
public class LockTraceManager {
  private final static Logger LOG = LoggerFactory.getLogger(LockPoolManager.class);

  private final HashMap<String, Tracker> threadCounts = new HashMap<>();

  /**
   * Puts the current thread into the tracking map.
   */
  public synchronized void putThreadName() {
    final String thread = getThreadName();
    Tracker tracker = threadCounts.compute(thread, (k, v) -> v == null ? new Tracker(thread) : v);
    tracker.incrLockCount();
  }

  /**
   * Removes the current thread from the tracking map.
   */
  public synchronized void removeThreadName() {
    final String thread = getThreadName();
    if (threadCounts.containsKey(thread)) {
      Tracker tracker = threadCounts.get(thread);
      if (tracker.shouldClear()) {
        threadCounts.remove(thread);
        return;
      }
      tracker.decrLockCount();
    }
  }

  /**
   * A class tracking how many locks this thread is holding.
   */
  private static class Tracker {
    private final Stack<Exception> logStack = new Stack<>();
    private int lockCount = 0;
    private final String threadName;

    Tracker(String threadName) {
      this.threadName = threadName;
    }

    public void incrLockCount() {
      logStack.push(new Exception("Lock stack trace"));
      lockCount++;
    }

    public void decrLockCount() {
      logStack.pop();
      lockCount--;
    }

    public void showLockMessage() {
      LOG.error("Thread={},count={}", threadName, lockCount);
      while (!logStack.isEmpty()) {
        Exception e = logStack.pop();
        LOG.error("Lock stack", e);
      }
    }

    public boolean shouldClear() {
      return lockCount == 1;
    }
  }

  public synchronized void checkForLeak() {
    if (threadCounts.isEmpty()) {
      LOG.warn("All locks have been released.");
      return;
    }
    for (Tracker tracker : threadCounts.values()) {
      tracker.showLockMessage();
    }
  }

  private String getThreadName() {
    return Thread.currentThread().getName() + Thread.currentThread().getId();
  }
}
