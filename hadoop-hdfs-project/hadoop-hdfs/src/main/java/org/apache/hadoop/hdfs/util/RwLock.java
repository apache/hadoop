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

/** Read-write lock interface. */
public interface RwLock {
  /** Acquire read lock. */
  public void readLock();

  /** Acquire read lock, unless interrupted while waiting  */
  void readLockInterruptibly() throws InterruptedException;

  /** Release read lock. */
  public void readUnlock();

  /** Check if the current thread holds read lock. */
  public boolean hasReadLock();

  /** Acquire write lock. */
  public void writeLock();
  
  /** Acquire write lock, unless interrupted while waiting  */
  void writeLockInterruptibly() throws InterruptedException;

  /** Release write lock. */
  public void writeUnlock();

  /** Check if the current thread holds write lock. */
  public boolean hasWriteLock();
}
