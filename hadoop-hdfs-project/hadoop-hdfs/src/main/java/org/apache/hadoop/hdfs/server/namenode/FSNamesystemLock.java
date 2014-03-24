
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

package org.apache.hadoop.hdfs.server.namenode;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;

/**
 * Mimics a ReentrantReadWriteLock so more sophisticated locking capabilities
 * are possible.
 */
class FSNamesystemLock implements ReadWriteLock {
  @VisibleForTesting
  protected ReentrantReadWriteLock coarseLock;
  
  /**
   * When locking the FSNS for a read that may take a long time, we take this
   * lock before taking the regular FSNS read lock. All writers also take this
   * lock before taking the FSNS write lock. Regular (short) readers do not
   * take this lock at all, instead relying solely on the synchronization of the
   * regular FSNS lock.
   * 
   * This scheme ensures that:
   * 1) In the case of normal (fast) ops, readers proceed concurrently and
   *    writers are not starved.
   * 2) In the case of long read ops, short reads are allowed to proceed
   *    concurrently during the duration of the long read.
   * 
   * See HDFS-5064 for more context.
   */
  @VisibleForTesting
  protected final ReentrantLock longReadLock = new ReentrantLock(true);
  
  FSNamesystemLock(boolean fair) {
    this.coarseLock = new ReentrantReadWriteLock(fair);
  }
  
  @Override
  public Lock readLock() {
    return coarseLock.readLock();
  }
  
  @Override
  public Lock writeLock() {
    return coarseLock.writeLock();
  }

  public Lock longReadLock() {
    return longReadLock;
  }
  
  public int getReadHoldCount() {
    return coarseLock.getReadHoldCount();
  }
  
  public int getWriteHoldCount() {
    return coarseLock.getWriteHoldCount();
  }
  
  public boolean isWriteLockedByCurrentThread() {
    return coarseLock.isWriteLockedByCurrentThread();
  }
}
