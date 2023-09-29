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

import static org.apache.hadoop.hdfs.DFSUtil.isParentEntry;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
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
   * @return All the mount quota paths.
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
   * Is the path a mount entry.
   *
   * @param path the path.
   * @return {@code true} if path is a mount entry; {@code false} otherwise.
   */
  boolean isMountEntry(String path) {
    readLock.lock();
    try {
      return this.cache.containsKey(path);
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
   * Get children paths (can include itself) under specified federation path.
   * @param parentPath Federated path.
   * @return Set of children paths.
   */
  public Set<String> getPaths(String parentPath) {
    readLock.lock();
    try {
      String from = parentPath;
      String to = parentPath + Character.MAX_VALUE;
      SortedMap<String, RouterQuotaUsage> subMap = this.cache.subMap(from, to);

      Set<String> validPaths = new HashSet<>();
      if (subMap != null) {
        for (String path : subMap.keySet()) {
          if (isParentEntry(path, parentPath)) {
            validPaths.add(path);
          }
        }
      }
      return validPaths;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get parent paths (including itself) and quotas of the specified federation
   * path. Only parents containing quota are returned.
   * @param childPath Federated path.
   * @return TreeMap of parent paths and quotas.
   */
  TreeMap<String, RouterQuotaUsage> getParentsContainingQuota(
      String childPath) {
    TreeMap<String, RouterQuotaUsage> res = new TreeMap<>();
    readLock.lock();
    try {
      Entry<String, RouterQuotaUsage> entry = this.cache.floorEntry(childPath);
      while (entry != null) {
        String mountPath = entry.getKey();
        RouterQuotaUsage quota = entry.getValue();
        if (isQuotaSet(quota) && isParentEntry(childPath, mountPath)) {
          res.put(mountPath, quota);
        }
        entry = this.cache.lowerEntry(mountPath);
      }
      return res;
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
   * Update quota in cache. The usage will be preserved.
   * @param path Mount table path.
   * @param quota Corresponding quota value.
   */
  public void updateQuota(String path, RouterQuotaUsage quota) {
    writeLock.lock();
    try {
      RouterQuotaUsage.Builder builder = new RouterQuotaUsage.Builder()
          .quota(quota.getQuota()).spaceQuota(quota.getSpaceQuota());
      RouterQuotaUsage current = this.cache.get(path);
      if (current != null) {
        builder.fileAndDirectoryCount(current.getFileAndDirectoryCount())
            .spaceConsumed(current.getSpaceConsumed());
      }
      this.cache.put(path, builder.build());
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
   * @param quota the quota usage.
   * @return True if the quota is set.
   */
  public static boolean isQuotaSet(QuotaUsage quota) {
    if (quota != null) {
      long nsQuota = quota.getQuota();
      long ssQuota = quota.getSpaceQuota();

      // once nsQuota or ssQuota was set, this mount table is quota set
      if (nsQuota != HdfsConstants.QUOTA_RESET
          || ssQuota != HdfsConstants.QUOTA_RESET || Quota.orByStorageType(
              t -> quota.getTypeQuota(t) != HdfsConstants.QUOTA_RESET)) {
        return true;
      }
    }

    return false;
  }
}
