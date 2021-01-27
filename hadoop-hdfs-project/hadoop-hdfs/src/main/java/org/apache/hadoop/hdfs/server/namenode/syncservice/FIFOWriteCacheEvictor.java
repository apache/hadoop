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
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evict write cache in FIFO order, applicable to provided storage write back
 * mount. The configurable lowWatermark and highWatermark are storage
 * usage ratios. Once highWatermark is reached, this cache evictor will
 * evict the cache till the lowWatermark is reached.
 */
public class FIFOWriteCacheEvictor extends WriteCacheEvictor
    implements CacheEvictionPolicy {
  protected final Logger log = LoggerFactory.getLogger(getClass());
  private float lowWatermark;
  private float highWatermark;

  public FIFOWriteCacheEvictor(Configuration conf, FSNamesystem fsNamesystem) {
    super(conf, fsNamesystem);
    highWatermark =
        conf.getFloat(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_HIGH_WATERMARK,
            DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_HIGH_WATERMARK_DEFAULT);
    lowWatermark =
        conf.getFloat(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_LOW_WATERMARK,
            DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_LOW_WATERMARK_DEFAULT);
    if (!isValidWatermark(lowWatermark, highWatermark)) {
      log.warn("invalid configuration for " +
          DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_HIGH_WATERMARK + " and " +
          DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_LOW_WATERMARK +
          ", will use default conf.");
      lowWatermark =
          DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_LOW_WATERMARK_DEFAULT;
      highWatermark =
          DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_HIGH_WATERMARK_DEFAULT;
    }
  }

  public static boolean isValidWatermark(float lowWatermark,
      float highWatermark) {
    if (lowWatermark > highWatermark) {
      return false;
    }
    if (lowWatermark < 0.0f || lowWatermark > 1.0f) {
      return false;
    }
    if (highWatermark < 0.0f || highWatermark > 1.0f) {
      return false;
    }
    return true;
  }

  @Override
  protected void evict() {
    removeCache();
  }

  protected Set<Long> removeCache() {
    Set<Long> blkCollectIdRemoved = new HashSet<>();
    long bytesToEvict = bytesToEvict();
    if (bytesToEvict <= 0 || evictQueue.isEmpty()) {
      return blkCollectIdRemoved;
    }
    while (bytesToEvict > 0 && !evictQueue.isEmpty()) {
      long blockCollectionId = evictQueue.poll();
      blkCollectIdRemoved.add(blockCollectionId);
      INodeFile iNodeFile = fsNamesystem.getBlockCollection(blockCollectionId);
      log.info("Trying to remove local blocks for " +
          iNodeFile.getFullPathName());
      removeLocalBlocks(iNodeFile.getBlocks());
      // Assume file replication is satisfied by current working datanodes.
      long freedSize =
          iNodeFile.getFileReplication() * iNodeFile.computeFileSize();
      bytesToEvict =
          bytesToEvict - freedSize;
    }
    return blkCollectIdRemoved;
  }

  /**
   * Return the bytes that need to be evicted.
   */
  @Override
  public long bytesToEvict() {
    long cacheUsed = getWriteCacheUsed();
    if (cacheUsed >= writeCacheQuota * highWatermark) {
      return cacheUsed - (long) (writeCacheQuota * lowWatermark);
    }
    return 0L;
  }

  public float getHighWatermark() {
    return highWatermark;
  }

  public float getLowWatermark() {
    return lowWatermark;
  }
}
