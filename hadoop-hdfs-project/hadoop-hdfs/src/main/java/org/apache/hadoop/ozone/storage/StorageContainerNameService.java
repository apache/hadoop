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

package org.apache.hadoop.ozone.storage;

import java.io.Closeable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.namenode.CacheManager;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;

/**
 * Namesystem implementation intended for use by StorageContainerManager.
 */
@InterfaceAudience.Private
public class StorageContainerNameService implements Namesystem, Closeable {

  private final ReentrantReadWriteLock coarseLock =
      new ReentrantReadWriteLock();
  private volatile boolean serviceRunning = true;

  @Override
  public boolean isRunning() {
    return serviceRunning;
  }

  @Override
  public BlockCollection getBlockCollection(long id) {
    // TBD
    return null;
  }

  @Override
  public void startSecretManagerIfNecessary() {
    // Secret manager is not supported
  }

  @Override
  public CacheManager getCacheManager() {
    // Centralized Cache Management is not supported
    return null;
  }

  @Override
  public HAContext getHAContext() {
    // HA mode is not supported
    return null;
  }

  @Override
  public boolean inTransitionToActive() {
    // HA mode is not supported
    return false;
  }

  @Override
  public boolean isInSnapshot(long blockCollectionID) {
    // Snapshots not supported
    return false;
  }

  @Override
  public void readLock() {
    coarseLock.readLock().lock();
  }

  @Override
  public void readUnlock() {
    coarseLock.readLock().unlock();
  }

  @Override
  public boolean hasReadLock() {
    return coarseLock.getReadHoldCount() > 0 || hasWriteLock();
  }

  @Override
  public void writeLock() {
    coarseLock.writeLock().lock();
  }

  @Override
  public void writeLockInterruptibly() throws InterruptedException {
    coarseLock.writeLock().lockInterruptibly();
  }

  @Override
  public void writeUnlock() {
    coarseLock.writeLock().unlock();
  }

  @Override
  public boolean hasWriteLock() {
    return coarseLock.isWriteLockedByCurrentThread();
  }

  @Override
  public boolean isInSafeMode() {
    // Safe mode is not supported
    return false;
  }

  @Override
  public boolean isInStartupSafeMode() {
    // Safe mode is not supported
    return false;
  }

  @Override
  public void close() {
    serviceRunning = false;
  }
}
