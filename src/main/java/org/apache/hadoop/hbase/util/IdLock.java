/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Allows multiple concurrent clients to lock on a numeric id with a minimal
 * memory overhead. The intended usage is as follows:
 *
 * <pre>
 * IdLock.Entry lockEntry = idLock.getLockEntry(id);
 * try {
 *   // User code.
 * } finally {
 *   idLock.releaseLockEntry(lockEntry);
 * }</pre>
 */
public class IdLock {

  /** An entry returned to the client as a lock object */
  public static class Entry {
    private final long id;
    private int numWaiters;
    private boolean isLocked = true;

    private Entry(long id) {
      this.id = id;
    }

    public String toString() {
      return "id=" + id + ", numWaiter=" + numWaiters + ", isLocked="
          + isLocked;
    }
  }

  private ConcurrentMap<Long, Entry> map =
      new ConcurrentHashMap<Long, Entry>();

  /**
   * Blocks until the lock corresponding to the given id is acquired.
   *
   * @param id an arbitrary number to lock on
   * @return an "entry" to pass to {@link #releaseLockEntry(Entry)} to release
   *         the lock
   * @throws IOException if interrupted
   */
  public Entry getLockEntry(long id) throws IOException {
    Entry entry = new Entry(id);
    Entry existing;
    while ((existing = map.putIfAbsent(entry.id, entry)) != null) {
      synchronized (existing) {
        if (existing.isLocked) {
          ++existing.numWaiters;  // Add ourselves to waiters.
          while (existing.isLocked) {
            try {
              existing.wait();
            } catch (InterruptedException e) {
              --existing.numWaiters;  // Remove ourselves from waiters.
              throw new InterruptedIOException(
                  "Interrupted waiting to acquire sparse lock");
            }
          }

          --existing.numWaiters;  // Remove ourselves from waiters.
          existing.isLocked = true;
          return existing;
        }
        // If the entry is not locked, it might already be deleted from the
        // map, so we cannot return it. We need to get our entry into the map
        // or get someone else's locked entry.
      }
    }
    return entry;
  }

  /**
   * Must be called in a finally block to decrease the internal counter and
   * remove the monitor object for the given id if the caller is the last
   * client.
   *
   * @param entry the return value of {@link #getLockEntry(long)}
   */
  public void releaseLockEntry(Entry entry) {
    synchronized (entry) {
      entry.isLocked = false;
      if (entry.numWaiters > 0) {
        entry.notify();
      } else {
        map.remove(entry.id);
      }
    }
  }

  /** For testing */
  void assertMapEmpty() {
    assert map.size() == 0;
  }

}
