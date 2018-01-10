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
package org.apache.hadoop.hdfs.server.federation.router;

import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

/**
 * Router quota manager in Router. The manager maintains
 * {@link RouterQuotaUsage} cache of mount tables and do management
 * for the quota caches.
 */
public class RouterQuotaManager {
  /** Quota usage <MountTable Path, Aggregated QuotaUsage> cache. */
  private TreeMap<String, RouterQuotaUsage> cache;

  /** Lock to access the quota cache. */
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  public RouterQuotaManager() {
    this.cache = new TreeMap<>();
  }

  /**
   * Get all the mount quota paths.
   */
  public Set<String> getAll() {
    readLock.lock();
    try {
      return this.cache.keySet();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get the nearest ancestor's quota usage, and meanwhile its quota was set.
   * @param path The path being written.
   * @return RouterQuotaUsage Quota usage.
   */
  public RouterQuotaUsage getQuotaUsage(String path) {
    readLock.lock();
    try {
      RouterQuotaUsage quotaUsage = this.cache.get(path);
      if (quotaUsage != null && isQuotaSet(quotaUsage)) {
        return quotaUsage;
      }

      // If not found, look for its parent path usage value.
      int pos = path.lastIndexOf(Path.SEPARATOR);
      if (pos != -1) {
        String parentPath = path.substring(0, pos);
        return getQuotaUsage(parentPath);
      }
    } finally {
      readLock.unlock();
    }

    return null;
  }

  /**
   * Get children paths (can including itself) under specified federation path.
   * @param parentPath
   * @return Set<String> Children path set.
   */
  public Set<String> getPaths(String parentPath) {
    readLock.lock();
    try {
      String from = parentPath;
      String to = parentPath + Character.MAX_VALUE;
      SortedMap<String, RouterQuotaUsage> subMap = this.cache.subMap(from, to);
      return subMap.keySet();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Put new entity into cache.
   * @param path Mount table path.
   * @param quotaUsage Corresponding cache value.
   */
  public void put(String path, RouterQuotaUsage quotaUsage) {
    writeLock.lock();
    try {
      this.cache.put(path, quotaUsage);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Remove the entity from cache.
   * @param path Mount table path.
   */
  public void remove(String path) {
    writeLock.lock();
    try {
      this.cache.remove(path);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Clean up the cache.
   */
  public void clear() {
    writeLock.lock();
    try {
      this.cache.clear();
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Check if the quota was set.
   * @param quota RouterQuotaUsage set in mount table.
   */
  public boolean isQuotaSet(RouterQuotaUsage quota) {
    if (quota != null) {
      long nsQuota = quota.getQuota();
      long ssQuota = quota.getSpaceQuota();

      // once nsQuota or ssQuota was set, this mount table is quota set
      if (nsQuota != HdfsConstants.QUOTA_DONT_SET
          || ssQuota != HdfsConstants.QUOTA_DONT_SET) {
        return true;
      }
    }

    return false;
  }
}
