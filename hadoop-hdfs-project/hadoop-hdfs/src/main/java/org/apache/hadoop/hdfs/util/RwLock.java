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
package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.hdfs.server.namenode.fgl.FSNamesystemLockMode;

/** Read-write lock interface for FSNamesystem. */
public interface RwLock {
  /** Acquire read lock. */
  default void readLock() {
    readLock(FSNamesystemLockMode.GLOBAL);
  }

  /** Acquire read lock. */
  void readLock(FSNamesystemLockMode lockMode);

  /** Acquire read lock, unless interrupted while waiting.  */
  default void readLockInterruptibly() throws InterruptedException {
    readLockInterruptibly(FSNamesystemLockMode.GLOBAL);
  }

  /** Acquire read lock, unless interrupted while waiting.  */
  void readLockInterruptibly(FSNamesystemLockMode lockMode) throws InterruptedException;

  /** Release read lock. */
  default void readUnlock() {
    readUnlock(FSNamesystemLockMode.GLOBAL, "OTHER");
  }

  /**
   * Release read lock with operation name.
   * @param opName Option name.
   */
  default void readUnlock(String opName) {
    readUnlock(FSNamesystemLockMode.GLOBAL, opName);
  }

  /**
   * Release read lock with operation name.
   * @param opName Option name.
   */
  void readUnlock(FSNamesystemLockMode lockMode, String opName);

  /** Check if the current thread holds read lock. */
  default boolean hasReadLock() {
    return hasReadLock(FSNamesystemLockMode.GLOBAL);
  }

  /** Check if the current thread holds read lock. */
  boolean hasReadLock(FSNamesystemLockMode lockMode);

  /** Acquire write lock. */
  default void writeLock() {
    writeLock(FSNamesystemLockMode.GLOBAL);
  }

  /** Acquire write lock. */
  void writeLock(FSNamesystemLockMode lockMode);
  
  /** Acquire write lock, unless interrupted while waiting.  */
  default void writeLockInterruptibly() throws InterruptedException {
    writeLockInterruptibly(FSNamesystemLockMode.GLOBAL);
  }

  /** Acquire write lock, unless interrupted while waiting.  */
  void writeLockInterruptibly(FSNamesystemLockMode lockMode) throws InterruptedException;

  /** Release write lock. */
  default void writeUnlock() {
    writeUnlock(FSNamesystemLockMode.GLOBAL, "OTHER");
  }

  /**
   * Release write lock with operation name.
   * @param opName Option name.
   */
  default void writeUnlock(String opName) {
    writeUnlock(FSNamesystemLockMode.GLOBAL, opName);
  }

  /**
   * Release write lock with operation name.
   * @param opName Option name.
   */
  void writeUnlock(FSNamesystemLockMode lockMode, String opName);

  /** Check if the current thread holds write lock. */
  default boolean hasWriteLock() {
    return hasWriteLock(FSNamesystemLockMode.GLOBAL);
  }

  /** Check if the current thread holds write lock. */
  boolean hasWriteLock(FSNamesystemLockMode lockMode);
}
