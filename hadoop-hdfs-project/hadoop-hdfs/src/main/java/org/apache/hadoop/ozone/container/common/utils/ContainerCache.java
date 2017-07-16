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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.utils.MetadataStore;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * container cache is a LRUMap that maintains the DB handles.
 */
public final class ContainerCache extends LRUMap {
  static final Log LOG = LogFactory.getLog(ContainerCache.class);
  private final Lock lock = new ReentrantLock();
  private static ContainerCache cache;
  private static final float LOAD_FACTOR = 0.75f;
  /**
   * Constructs a cache that holds DBHandle references.
   */
  private ContainerCache(int maxSize, float loadFactor, boolean
      scanUntilRemovable) {
    super(maxSize, loadFactor, scanUntilRemovable);
  }

  /**
   * Return a singleton instance of {@link ContainerCache}
   * that holds the DB handlers.
   *
   * @param conf - Configuration.
   * @return A instance of {@link ContainerCache}.
   */
  public synchronized static ContainerCache getInstance(Configuration conf) {
    if (cache == null) {
      int cacheSize = conf.getInt(OzoneConfigKeys.OZONE_KEY_CACHE,
          OzoneConfigKeys.OZONE_KEY_CACHE_DEFAULT);
      cache = new ContainerCache(cacheSize, LOAD_FACTOR, true);
    }
    return cache;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean removeLRU(LinkEntry entry) {
    lock.lock();
    try {
      MetadataStore db = (MetadataStore) entry.getValue();
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
   * @return MetadataStore.
   */
  public MetadataStore getDB(String containerName) {
    Preconditions.checkNotNull(containerName);
    Preconditions.checkState(!containerName.isEmpty());
    lock.lock();
    try {
      return (MetadataStore) this.get(containerName);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove a DB handler from cache.
   *
   * @param containerName - Name of the container.
   */
  public void removeDB(String containerName) {
    Preconditions.checkNotNull(containerName);
    Preconditions.checkState(!containerName.isEmpty());
    lock.lock();
    try {
      MetadataStore db = this.getDB(containerName);
      if (db != null) {
        try {
          db.close();
        } catch (IOException e) {
          LOG.warn("There is some issue to stop an unused DB handler.", e);
        }
      }
      this.remove(containerName);
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
  public void putDB(String containerName, MetadataStore db) {
    Preconditions.checkNotNull(containerName);
    Preconditions.checkState(!containerName.isEmpty());
    lock.lock();
    try {
      this.put(containerName, db);
    } finally {
      lock.unlock();
    }
  }
}
