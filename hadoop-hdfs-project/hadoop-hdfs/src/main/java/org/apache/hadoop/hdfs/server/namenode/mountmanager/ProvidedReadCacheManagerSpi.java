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
package org.apache.hadoop.hdfs.server.namenode.mountmanager;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.List;

/**
 * Interface defining the cache manager used for external, Provided stores.
 * The read cache management is used for provided storage readOnly/writeBack
 * mount. For writeBack mount, after block is synced to remote storage and
 * local blocks are evicted, the read through style read can also cache remote
 * data to local HDFS.
 */
@InterfaceAudience.Private
public interface ProvidedReadCacheManagerSpi {

  /**
   * Factory for creating {@link ProvidedReadCacheManagerSpi} objects.
   * @param <T>
   */
  abstract class Factory<T extends ProvidedReadCacheManagerSpi> {

    /**
     * @param conf Configuration.
     * @return the configured factory.
     */
    public static Factory<?> getFactory(Configuration conf) {
      String className = conf.get(
          DFSConfigKeys.DFS_PROVIDED_READ_CACHE_MANAGER_FACTORY,
          DFSConfigKeys.DFS_PROVIDED_READ_CACHE_MANAGER_FACTORY_DEFAULT);

      final Class<? extends Factory> clazz;
      try {
        clazz = Class.forName(className).asSubclass(Factory.class);
        return ReflectionUtils.newInstance(clazz, conf);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(e);
      }
    }

    /** Create a new object. */
    public abstract T newInstance(Configuration conf, FSNamesystem namesystem,
        BlockManager blockManager);
  }

  /**
   * Add block to set of blocks cached.
   *
   * @param blockInfo BlockInfo added to cache.
   * @param storageInfo the {@link DatanodeStorageInfo} the block is cached on.
   */
  default void addCachedBlock(BlockInfo blockInfo,
      DatanodeStorageInfo storageInfo) {
  }

  /**
   * Remove blocks from set of blocks cached.
   *
   * @param blocks list of BlockInfos to remove.
   */
  default void removeCachedBlocks(List<BlockInfo> blocks) {
  }

  /**
   * Remove block from set of blocks cached.
   *
   * @param blockInfo BlockInfo to remove.
   * @param dnDesc the {@link DatanodeDescriptor} the block is removed from.
   */
  default void removeCachedBlock(BlockInfo blockInfo,
      DatanodeDescriptor dnDesc) {
  }

  /**
   * Add a mount path which needs to potentially managed.
   * This is useful to cache managers which evict blocks based on information
   * on files (e.g., access time used for LRU).
   *
   * @param mountPath
   * @param volInfo
   */
  default void init(Path mountPath, ProvidedVolumeInfo volInfo) {
  }

  /**
   * Evict blocks based on the cache policy.
   */
  default void evictBlocks() {
  }

  /**
   * Start the cache monitoring thread.
   */
  default void startService() {
  }

  /**
   * Stop the cache monitoring thread.
   */
  default void stopService() {
  }

  /**
   * @return total space used to cache blocks, in bytes.
   */
  default long getCacheUsedForProvided() {
    return 0L;
  }

  /**
   * @return capacity, in bytes, which can be used to cache Provided data.
   */
  default long getCacheCapacityForProvided() {
    return 0L;
  }
}
