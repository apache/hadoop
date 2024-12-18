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

/** Read-write lock interface for FSNamesystem. */
public interface RwLock {
  /** Acquire read lock. */
  default void readLock() {
    readLock(RwLockMode.GLOBAL);
  }

  /** Acquire read lock. */
  void readLock(RwLockMode lockMode);

  /** Acquire read lock, unless interrupted while waiting.  */
  default void readLockInterruptibly() throws InterruptedException {
    readLockInterruptibly(RwLockMode.GLOBAL);
  }

  /** Acquire read lock, unless interrupted while waiting.  */
  void readLockInterruptibly(RwLockMode lockMode) throws InterruptedException;

  /** Release read lock. */
  default void readUnlock() {
    readUnlock(RwLockMode.GLOBAL, "OTHER");
  }

  /**
   * Release read lock with operation name.
   * @param opName Option name.
   */
  default void readUnlock(String opName) {
    readUnlock(RwLockMode.GLOBAL, opName);
  }

  /**
   * Release read lock with operation name.
   * @param opName Option name.
   */
  void readUnlock(RwLockMode lockMode, String opName);

  /** Check if the current thread holds read lock. */
  default boolean hasReadLock() {
    return hasReadLock(RwLockMode.GLOBAL);
  }

  /** Check if the current thread holds read lock. */
  boolean hasReadLock(RwLockMode lockMode);

  /** Acquire write lock. */
  default void writeLock() {
    writeLock(RwLockMode.GLOBAL);
  }

  /** Acquire write lock. */
  void writeLock(RwLockMode lockMode);
  
  /** Acquire write lock, unless interrupted while waiting.  */
  default void writeLockInterruptibly() throws InterruptedException {
    writeLockInterruptibly(RwLockMode.GLOBAL);
  }

  /** Acquire write lock, unless interrupted while waiting.  */
  void writeLockInterruptibly(RwLockMode lockMode) throws InterruptedException;

  /** Release write lock. */
  default void writeUnlock() {
    writeUnlock(RwLockMode.GLOBAL, "OTHER");
  }

  /**
   * Release write lock with operation name.
   * @param opName Option name.
   */
  default void writeUnlock(String opName) {
    writeUnlock(RwLockMode.GLOBAL, opName);
  }

  /**
   * Release write lock with operation name.
   * @param opName Option name.
   */
  void writeUnlock(RwLockMode lockMode, String opName);

  /** Check if the current thread holds write lock. */
  default boolean hasWriteLock() {
    return hasWriteLock(RwLockMode.GLOBAL);
  }

  /** Check if the current thread holds write lock. */
  boolean hasWriteLock(RwLockMode lockMode);
}
