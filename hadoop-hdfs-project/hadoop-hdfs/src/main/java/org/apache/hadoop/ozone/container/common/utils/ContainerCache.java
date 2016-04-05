/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * container cache is a LRUMap that maintains the DB handles.
 */
public class ContainerCache extends LRUMap {
  static final Log LOG = LogFactory.getLog(ContainerCache.class);
  private final Lock lock = new ReentrantLock();

  /**
   * Constructs a cache that holds DBHandle references.
   */
  public ContainerCache(int maxSize, float loadFactor, boolean
      scanUntilRemovable) {
    super(maxSize, loadFactor, scanUntilRemovable);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean removeLRU(LinkEntry entry) {
    lock.lock();
    try {
      LevelDBStore db = (LevelDBStore) entry.getValue();
      db.close();
    } catch (IOException e) {
      LOG.error("Error closing DB. Container: " + entry.getKey().toString(), e);
    } finally {
      lock.unlock();
    }
    return true;
  }

  /**
   * Returns a DB handle if available, null otherwise.
   *
   * @param containerName - Name of the container.
   * @return OzoneLevelDBStore.
   */
  public LevelDBStore getDB(String containerName) {
    Preconditions.checkNotNull(containerName);
    Preconditions.checkState(!containerName.isEmpty());
    lock.lock();
    try {
      return (LevelDBStore) this.get(containerName);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Add a new DB to the cache.
   *
   * @param containerName - Name of the container
   * @param db            - DB handle
   */
  public void putDB(String containerName, LevelDBStore db) {
    Preconditions.checkNotNull(containerName);
    Preconditions.checkState(!containerName.isEmpty());
    lock.lock();
    try {
      this.put(containerName, db);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove an entry from the cache.
   *
   * @param containerName - Name of the container.
   */
  public void removeDB(String containerName) {
    Preconditions.checkNotNull(containerName);
    Preconditions.checkState(!containerName.isEmpty());
    lock.lock();
    try {
      this.remove(containerName);
    } finally {
      lock.unlock();
    }
  }
}
