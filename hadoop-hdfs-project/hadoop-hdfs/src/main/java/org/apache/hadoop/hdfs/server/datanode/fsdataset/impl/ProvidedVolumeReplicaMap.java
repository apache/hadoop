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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBuilder;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.util.AutoCloseableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_REPLICAMAP_CACHE_ENABLE_STATS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_REPLICAMAP_CACHE_ENABLE_STATS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_REPLICAMAP_CACHE_SIZE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_REPLICAMAP_CACHE_SIZE_DEFAULT;

/**
 * Maintains the replica map for Provided volumes.
 * This implementation of {@link VolumeReplicaMap} loads replicas on-demand
 * using the configured {@link BlockAliasMap}.
 */
class ProvidedVolumeReplicaMap extends VolumeReplicaMap {

  public static final Logger LOG =
      LoggerFactory.getLogger(ProvidedVolumeReplicaMap.class);

  private BlockAliasMap.Reader<FileRegion> aliasMapReader;
  private final FsVolumeImpl volume;
  private final DataNodeMetrics dnMetrics;
  private final Configuration conf;
  private final FileSystem remoteFS;
  // map of block pool id to a cache holding the ReplicaInfos.
  private final Map<String, LoadingCache<Long, ReplicaInfo>> map =
      new HashMap<>();
  private final long providedReplicaCacheSize;
  private final boolean enableCacheStats;

  ProvidedVolumeReplicaMap(BlockAliasMap.Reader<FileRegion> aliasMapReader,
      FsVolumeImpl volume, DataNodeMetrics dnMetrics,
      Configuration conf, FileSystem remoteFS) {
    super(new AutoCloseableLock());
    this.aliasMapReader = aliasMapReader;
    this.volume = volume;
    this.dnMetrics = dnMetrics;
    this.conf = conf;
    this.remoteFS = remoteFS;
    providedReplicaCacheSize = conf.getLong(DFS_PROVIDED_REPLICAMAP_CACHE_SIZE,
        DFS_PROVIDED_REPLICAMAP_CACHE_SIZE_DEFAULT);
    enableCacheStats = conf.getBoolean(
        DFS_PROVIDED_REPLICAMAP_CACHE_ENABLE_STATS,
        DFS_PROVIDED_REPLICAMAP_CACHE_ENABLE_STATS_DEFAULT);
  }

  @Override
  String[] getBlockPoolList() {
    try (AutoCloseableLock l = lock.acquire()) {
      return map.keySet().toArray(new String[map.keySet().size()]);
    }
  }

  @Override
  ReplicaInfo get(String bpid, Block block) {
    checkBlock(block);
    return get(bpid, block.getBlockId());
  }

  private LoadingCache<Long, ReplicaInfo> initCache() {
    CacheBuilder builder = CacheBuilder.newBuilder()
        .maximumSize(providedReplicaCacheSize);
    return builder.build(
          new CacheLoader<Long, ReplicaInfo>() {
            @Override
            public ReplicaInfo load(Long blockId) throws ExecutionException {
              try {
                return getReplicaFromAliasMap(blockId);
              } catch (IOException e) {
                throw new ExecutionException(e);
              }
            }
          });
  }

  @Override
  ReplicaInfo get(String bpid, long blockId) {
    checkBlockPool(bpid);
    // look up in the cache.
    try (AutoCloseableLock l = lock.acquire()) {
      LoadingCache<Long, ReplicaInfo> cache = map.get(bpid);
      if (cache != null) {
        try {
          if (enableCacheStats) {
            dnMetrics.incrAliasMapLookUps();
            dnMetrics.setAliasMapCacheHitRate((float) cache.stats().hitRate());
          }
          return cache.get(blockId);
        } catch (ExecutionException e) {
          LOG.warn("Exception in retrieving ReplicaInfo for block id {}:\n{}",
                  blockId, e.getMessage());
        }
      }
      return null;
    }
  }

  private ReplicaInfo getReplicaFromAliasMap(long blockId) throws IOException {
    Optional<FileRegion> region = aliasMapReader.resolve(blockId);
    if (region.isPresent()) {
      return new ReplicaBuilder(
          HdfsServerConstants.ReplicaState.FINALIZED)
          .setFileRegion(region.get())
          .setConf(conf)
          .setFsVolume(volume)
          .setRemoteFS(remoteFS)
          .build();
    } else {
      throw new ReplicaNotFoundException(
          "Replica for block " + blockId + " not found in AliasMap");
    }
  }

  @Override
  Collection<ReplicaInfo> replicas(String bpid) {
    try (AutoCloseableLock l = lock.acquire()) {
      // only return the replicas that have been paged in so far.
      LoadingCache<Long, ReplicaInfo> cache = map.get(bpid);
      return cache != null ? cache.asMap().values() : null;
    }
  }

  @Override
  void initBlockPool(String bpid) {
    checkBlockPool(bpid);
    try (AutoCloseableLock l = lock.acquire()) {
      LoadingCache<Long, ReplicaInfo> cache = map.get(bpid);
      if (cache == null) {
        // Add an entry for block pool if it does not exist already
        cache = initCache();
        map.put(bpid, cache);
      }
    }
  }

  @Override
  void addAll(VolumeReplicaMap other) {
    try (AutoCloseableLock l = lock.acquire()) {
      if (other instanceof ProvidedVolumeReplicaMap) {
        try (AutoCloseableLock otherL =
            ((ProvidedVolumeReplicaMap) other).lock.acquire()) {
          map.putAll(((ProvidedVolumeReplicaMap) other).map);
        }
      } else {
        throw new UnsupportedOperationException(
            "ProvidedVolumeReplicaMap cannot be merged with" +
                "other LeafReplicaMaps");
      }
    }
  }

  @Override
  ReplicaInfo add(String bpid, ReplicaInfo replicaInfo) {
    throw new UnsupportedOperationException(
        "ProvidedVolumeReplicaMap is read-only; cannot add replicas");
  }

  @Override
  ReplicaInfo remove(String bpid, long blockId) {
    // remove is called when a PROVIDED block is being cached.
    return null;
  }

  @Override
  ReplicaInfo remove(String bpid, Block block) {
    // remove is called when a PROVIDED block is being cached.
    return null;
  }

  @Override
  int size(String bpid) {
    try (AutoCloseableLock l = lock.acquire()) {
      LoadingCache<Long, ReplicaInfo> cache = map.get(bpid);
      return cache != null ? (int) cache.size() : 0;
    }
  }

  @Override
  void cleanUpBlockPool(String bpid) {
    checkBlockPool(bpid);
    try (AutoCloseableLock l = lock.acquire()) {
      map.remove(bpid);
    }
  }
}
