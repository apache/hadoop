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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.InvalidPoolError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.PoolWritePermissionDeniedError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.UnexpectedAddPathCacheDirectiveException;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.PathCacheDirective;
import org.apache.hadoop.hdfs.protocol.PathCacheEntry;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.InvalidIdException;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.NoSuchIdException;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.RemovePermissionDeniedException;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.UnexpectedRemovePathCacheEntryException;
import org.apache.hadoop.util.Fallible;

/**
 * The Cache Manager handles caching on DataNodes.
 */
final class CacheManager {
  public static final Log LOG = LogFactory.getLog(CacheManager.class);

  /**
   * Cache entries, sorted by ID.
   *
   * listPathCacheEntries relies on the ordering of elements in this map 
   * to track what has already been listed by the client.
   */
  private final TreeMap<Long, PathCacheEntry> entriesById =
      new TreeMap<Long, PathCacheEntry>();

  /**
   * Cache entries, sorted by directive.
   */
  private final TreeMap<PathCacheDirective, PathCacheEntry> entriesByDirective =
      new TreeMap<PathCacheDirective, PathCacheEntry>();

  /**
   * Cache pools, sorted by name.
   */
  private final TreeMap<String, CachePool> cachePoolsByName =
      new TreeMap<String, CachePool>();

  /**
   * Cache pools, sorted by ID
   */
  private final TreeMap<Long, CachePool> cachePoolsById =
      new TreeMap<Long, CachePool>();

  /**
   * The entry ID to use for a new entry.
   */
  private long nextEntryId;

  /**
   * The pool ID to use for a new pool.
   */
  private long nextPoolId;

  CacheManager(FSDirectory dir, Configuration conf) {
    // TODO: support loading and storing of the CacheManager state
    clear();
  }

  synchronized void clear() {
    entriesById.clear();
    entriesByDirective.clear();
    cachePoolsByName.clear();
    cachePoolsById.clear();
    nextEntryId = 1;
    nextPoolId = 1;
  }

  synchronized long getNextEntryId() throws IOException {
    if (nextEntryId == Long.MAX_VALUE) {
      throw new IOException("no more available entry IDs");
    }
    return nextEntryId++;
  }

  synchronized long getNextPoolId() throws IOException {
    if (nextPoolId == Long.MAX_VALUE) {
      throw new IOException("no more available pool IDs");
    }
    return nextPoolId++;
  }

  private synchronized Fallible<PathCacheEntry> addDirective(
        FSPermissionChecker pc, PathCacheDirective directive) {
    CachePool pool = cachePoolsById.get(directive.getPoolId());
    if (pool == null) {
      LOG.info("addDirective " + directive + ": pool not found.");
      return new Fallible<PathCacheEntry>(
          new InvalidPoolError(directive));
    }
    if (!pc.checkPermission(pool, FsAction.WRITE)) {
      LOG.info("addDirective " + directive + ": write permission denied.");
      return new Fallible<PathCacheEntry>(
          new PoolWritePermissionDeniedError(directive));
    }
    try {
      directive.validate();
    } catch (IOException ioe) {
      LOG.info("addDirective " + directive + ": validation failed.");
      return new Fallible<PathCacheEntry>(ioe);
    }
    // Check if we already have this entry.
    PathCacheEntry existing = entriesByDirective.get(directive);
    if (existing != null) {
      // Entry already exists: return existing entry.
      LOG.info("addDirective " + directive + ": there is an " +
          "existing directive " + existing);
      return new Fallible<PathCacheEntry>(existing);
    }
    // Add a new entry with the next available ID.
    PathCacheEntry entry;
    try {
      entry = new PathCacheEntry(getNextEntryId(), directive);
    } catch (IOException ioe) {
      return new Fallible<PathCacheEntry>(
          new UnexpectedAddPathCacheDirectiveException(directive));
    }
    LOG.info("addDirective " + directive + ": added cache directive "
        + directive);
    entriesByDirective.put(directive, entry);
    entriesById.put(entry.getEntryId(), entry);
    return new Fallible<PathCacheEntry>(entry);
  }

  public synchronized List<Fallible<PathCacheEntry>> addDirectives(
      FSPermissionChecker pc, List<PathCacheDirective> directives) {
    ArrayList<Fallible<PathCacheEntry>> results = 
        new ArrayList<Fallible<PathCacheEntry>>(directives.size());
    for (PathCacheDirective directive: directives) {
      results.add(addDirective(pc, directive));
    }
    return results;
  }

  private synchronized Fallible<Long> removeEntry(FSPermissionChecker pc,
        long entryId) {
    // Check for invalid IDs.
    if (entryId <= 0) {
      LOG.info("removeEntry " + entryId + ": invalid non-positive entry ID.");
      return new Fallible<Long>(new InvalidIdException(entryId));
    }
    // Find the entry.
    PathCacheEntry existing = entriesById.get(entryId);
    if (existing == null) {
      LOG.info("removeEntry " + entryId + ": entry not found.");
      return new Fallible<Long>(new NoSuchIdException(entryId));
    }
    CachePool pool = cachePoolsById.get(existing.getDirective().getPoolId());
    if (pool == null) {
      LOG.info("removeEntry " + entryId + ": pool not found for directive " +
        existing.getDirective());
      return new Fallible<Long>(
          new UnexpectedRemovePathCacheEntryException(entryId));
    }
    if (!pc.checkPermission(pool, FsAction.WRITE)) {
      LOG.info("removeEntry " + entryId + ": write permission denied to " +
          "pool " + pool + " for entry " + existing);
      return new Fallible<Long>(
          new RemovePermissionDeniedException(entryId));
    }

    // Remove the corresponding entry in entriesByDirective.
    if (entriesByDirective.remove(existing.getDirective()) == null) {
      LOG.warn("removeEntry " + entryId + ": failed to find existing entry " +
          existing + " in entriesByDirective");
      return new Fallible<Long>(
          new UnexpectedRemovePathCacheEntryException(entryId));
    }
    entriesById.remove(entryId);
    return new Fallible<Long>(entryId);
  }

  public synchronized List<Fallible<Long>> removeEntries(FSPermissionChecker pc,
      List<Long> entryIds) {
    ArrayList<Fallible<Long>> results = 
        new ArrayList<Fallible<Long>>(entryIds.size());
    for (Long entryId : entryIds) {
      results.add(removeEntry(pc, entryId));
    }
    return results;
  }

  public synchronized List<PathCacheEntry> listPathCacheEntries(
      FSPermissionChecker pc, long prevId, Long poolId, int maxReplies) {
    final int MAX_PRE_ALLOCATED_ENTRIES = 16;
    ArrayList<PathCacheEntry> replies = new ArrayList<PathCacheEntry>(
        Math.min(MAX_PRE_ALLOCATED_ENTRIES, maxReplies));
    int numReplies = 0;
    SortedMap<Long, PathCacheEntry> tailMap = entriesById.tailMap(prevId + 1);
    for (PathCacheEntry entry : tailMap.values()) {
      if (numReplies >= maxReplies) {
        return replies;
      }
      long entryPoolId = entry.getDirective().getPoolId();
      if (poolId == null || poolId <= 0 || entryPoolId == poolId) {
        if (pc.checkPermission(
            cachePoolsById.get(entryPoolId), FsAction.EXECUTE)) {
          replies.add(entry);
          numReplies++;
        }
      }
    }
    return replies;
  }

  synchronized CachePool getCachePool(long id) {
    return cachePoolsById.get(id);
  }

  /**
   * Create a cache pool.
   * 
   * Only the superuser should be able to call this function.
   *
   * @param info
   *          The info for the cache pool to create.
   * @return created CachePool
   */
  public synchronized CachePool addCachePool(CachePoolInfo info)
      throws IOException {
    String poolName = info.getPoolName();
    if (poolName == null || poolName.isEmpty()) {
      throw new IOException("invalid empty cache pool name");
    }
    if (cachePoolsByName.containsKey(poolName)) {
      throw new IOException("cache pool " + poolName + " already exists.");
    }
    CachePool cachePool = new CachePool(getNextPoolId(), poolName,
      info.getOwnerName(), info.getGroupName(), info.getMode(),
      info.getWeight());
    cachePoolsById.put(cachePool.getId(), cachePool);
    cachePoolsByName.put(poolName, cachePool);
    LOG.info("created new cache pool " + cachePool);
    return cachePool;
  }

  /**
   * Modify a cache pool.
   * 
   * Only the superuser should be able to call this function.
   *
   * @param info
   *          The info for the cache pool to modify.
   */
  public synchronized void modifyCachePool(long poolId, CachePoolInfo info)
      throws IOException {
    if (poolId <= 0) {
      throw new IOException("invalid pool id " + poolId);
    }
    if (!cachePoolsById.containsKey(poolId)) {
      throw new IOException("cache pool id " + poolId + " does not exist.");
    }
    CachePool pool = cachePoolsById.get(poolId);
    // Remove the old CachePoolInfo
    removeCachePool(poolId);
    // Build up the new CachePoolInfo
    CachePoolInfo.Builder newInfo = CachePoolInfo.newBuilder(pool.getInfo());
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    if (info.getPoolName() != null) {
      newInfo.setPoolName(info.getPoolName());
      bld.append(prefix).
      append("set name to ").append(info.getOwnerName());
      prefix = "; ";
    }
    if (info.getOwnerName() != null) {
      newInfo.setOwnerName(info.getOwnerName());
      bld.append(prefix).
        append("set owner to ").append(info.getOwnerName());
      prefix = "; ";
    }
    if (info.getGroupName() != null) {
      newInfo.setGroupName(info.getGroupName());
      bld.append(prefix).
        append("set group to ").append(info.getGroupName());
      prefix = "; ";
    }
    if (info.getMode() != null) {
      newInfo.setMode(info.getMode());
      bld.append(prefix).
        append(String.format("set mode to ", info.getMode()));
      prefix = "; ";
    }
    if (info.getWeight() != null) {
      newInfo.setWeight(info.getWeight());
      bld.append(prefix).
        append("set weight to ").append(info.getWeight());
      prefix = "; ";
    }
    if (prefix.isEmpty()) {
      bld.append("no changes.");
    } else {
      pool.setInfo(newInfo.build());
    }
    // Put the newly modified info back in
    cachePoolsById.put(poolId, pool);
    cachePoolsByName.put(info.getPoolName(), pool);
    LOG.info("modified pool id " + pool.getId()
        + " (" + pool.getInfo().getPoolName() + "); "
        + bld.toString());
  }

  /**
   * Remove a cache pool.
   * 
   * Only the superuser should be able to call this function.
   *
   * @param poolId
   *          The id of the cache pool to remove.
   */
  public synchronized void removeCachePool(long poolId) throws IOException {
    if (!cachePoolsById.containsKey(poolId)) {
      throw new IOException("can't remove nonexistent cache pool id " + poolId);
    }
    // Remove all the entries associated with the pool
    Iterator<Map.Entry<Long, PathCacheEntry>> it =
        entriesById.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Long, PathCacheEntry> entry = it.next();
      if (entry.getValue().getDirective().getPoolId() == poolId) {
        it.remove();
        entriesByDirective.remove(entry.getValue().getDirective());
      }
    }
    // Remove the pool
    CachePool pool = cachePoolsById.remove(poolId);
    cachePoolsByName.remove(pool.getInfo().getPoolName());
  }

  public synchronized List<CachePool> listCachePools(Long prevKey,
      int maxRepliesPerRequest) {
    final int MAX_PREALLOCATED_REPLIES = 16;
    ArrayList<CachePool> results = 
        new ArrayList<CachePool>(Math.min(MAX_PREALLOCATED_REPLIES,
            maxRepliesPerRequest));
    SortedMap<Long, CachePool> tailMap =
        cachePoolsById.tailMap(prevKey, false);
    for (CachePool pool : tailMap.values()) {
      results.add(pool);
    }
    return results;
  }
}
