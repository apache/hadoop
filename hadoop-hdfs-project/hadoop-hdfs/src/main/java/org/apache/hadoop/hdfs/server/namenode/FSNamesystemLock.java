
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
