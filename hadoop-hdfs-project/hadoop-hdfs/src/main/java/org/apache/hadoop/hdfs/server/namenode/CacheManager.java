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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES_DEFAULT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDirective;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheEntry;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.InvalidPoolNameError;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.UnexpectedAddPathBasedCacheDirectiveException;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.PoolWritePermissionDeniedError;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheEntryException.InvalidIdException;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheEntryException.NoSuchIdException;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheEntryException.UnexpectedRemovePathBasedCacheEntryException;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheEntryException.RemovePermissionDeniedException;
import org.apache.hadoop.util.Fallible;

/**
 * The Cache Manager handles caching on DataNodes.
 */
final class CacheManager {
  public static final Log LOG = LogFactory.getLog(CacheManager.class);

  /**
   * Cache entries, sorted by ID.
   *
   * listPathBasedCacheEntries relies on the ordering of elements in this map 
   * to track what has already been listed by the client.
   */
  private final TreeMap<Long, PathBasedCacheEntry> entriesById =
      new TreeMap<Long, PathBasedCacheEntry>();

  /**
   * Cache entries, sorted by directive.
   */
  private final TreeMap<PathBasedCacheDirective, PathBasedCacheEntry> entriesByDirective =
      new TreeMap<PathBasedCacheDirective, PathBasedCacheEntry>();

  /**
   * Cache pools, sorted by name.
   */
  private final TreeMap<String, CachePool> cachePools =
      new TreeMap<String, CachePool>();

  /**
   * The entry ID to use for a new entry.
   */
  private long nextEntryId;

  /**
   * Maximum number of cache pools to list in one operation.
   */
  private final int maxListCachePoolsResponses;

  /**
   * Maximum number of cache pool directives to list in one operation.
   */
  private final int maxListCacheDirectivesResponses;

  CacheManager(FSDirectory dir, Configuration conf) {
    // TODO: support loading and storing of the CacheManager state
    clear();
    maxListCachePoolsResponses = conf.getInt(
        DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES,
        DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT);
    maxListCacheDirectivesResponses = conf.getInt(
        DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES,
        DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES_DEFAULT);
  }

  synchronized void clear() {
    entriesById.clear();
    entriesByDirective.clear();
    cachePools.clear();
    nextEntryId = 1;
  }

  synchronized long getNextEntryId() throws IOException {
    if (nextEntryId == Long.MAX_VALUE) {
      throw new IOException("no more available IDs");
    }
    return nextEntryId++;
  }

  private synchronized Fallible<PathBasedCacheEntry> addDirective(
        PathBasedCacheDirective directive, FSPermissionChecker pc) {
    CachePool pool = cachePools.get(directive.getPool());
    if (pool == null) {
      LOG.info("addDirective " + directive + ": pool not found.");
      return new Fallible<PathBasedCacheEntry>(
          new InvalidPoolNameError(directive));
    }
    if ((pc != null) && (!pc.checkPermission(pool, FsAction.WRITE))) {
      LOG.info("addDirective " + directive + ": write permission denied.");
      return new Fallible<PathBasedCacheEntry>(
          new PoolWritePermissionDeniedError(directive));
    }
    try {
      directive.validate();
    } catch (IOException ioe) {
      LOG.info("addDirective " + directive + ": validation failed.");
      return new Fallible<PathBasedCacheEntry>(ioe);
    }
    // Check if we already have this entry.
    PathBasedCacheEntry existing = entriesByDirective.get(directive);
    if (existing != null) {
      // Entry already exists: return existing entry.
      LOG.info("addDirective " + directive + ": there is an " +
          "existing directive " + existing);
      return new Fallible<PathBasedCacheEntry>(existing);
    }
    // Add a new entry with the next available ID.
    PathBasedCacheEntry entry;
    try {
      entry = new PathBasedCacheEntry(getNextEntryId(), directive);
    } catch (IOException ioe) {
      return new Fallible<PathBasedCacheEntry>(
          new UnexpectedAddPathBasedCacheDirectiveException(directive));
    }
    LOG.info("addDirective " + directive + ": added cache directive "
        + directive);
    entriesByDirective.put(directive, entry);
    entriesById.put(entry.getEntryId(), entry);
    return new Fallible<PathBasedCacheEntry>(entry);
  }

  public synchronized List<Fallible<PathBasedCacheEntry>> addDirectives(
      List<PathBasedCacheDirective> directives, FSPermissionChecker pc) {
    ArrayList<Fallible<PathBasedCacheEntry>> results = 
        new ArrayList<Fallible<PathBasedCacheEntry>>(directives.size());
    for (PathBasedCacheDirective directive: directives) {
      results.add(addDirective(directive, pc));
    }
    return results;
  }

  private synchronized Fallible<Long> removeEntry(long entryId,
        FSPermissionChecker pc) {
    // Check for invalid IDs.
    if (entryId <= 0) {
      LOG.info("removeEntry " + entryId + ": invalid non-positive entry ID.");
      return new Fallible<Long>(new InvalidIdException(entryId));
    }
    // Find the entry.
    PathBasedCacheEntry existing = entriesById.get(entryId);
    if (existing == null) {
      LOG.info("removeEntry " + entryId + ": entry not found.");
      return new Fallible<Long>(new NoSuchIdException(entryId));
    }
    CachePool pool = cachePools.get(existing.getDirective().getPool());
    if (pool == null) {
      LOG.info("removeEntry " + entryId + ": pool not found for directive " +
        existing.getDirective());
      return new Fallible<Long>(
          new UnexpectedRemovePathBasedCacheEntryException(entryId));
    }
    if ((pc != null) && (!pc.checkPermission(pool, FsAction.WRITE))) {
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
          new UnexpectedRemovePathBasedCacheEntryException(entryId));
    }
    entriesById.remove(entryId);
    return new Fallible<Long>(entryId);
  }

  public synchronized List<Fallible<Long>> removeEntries(List<Long> entryIds,
      FSPermissionChecker pc) {
    ArrayList<Fallible<Long>> results = 
        new ArrayList<Fallible<Long>>(entryIds.size());
    for (Long entryId : entryIds) {
      results.add(removeEntry(entryId, pc));
    }
    return results;
  }

  public synchronized BatchedListEntries<PathBasedCacheEntry> 
        listPathBasedCacheEntries(long prevId, String filterPool,
            String filterPath, FSPermissionChecker pc) throws IOException {
    final int NUM_PRE_ALLOCATED_ENTRIES = 16;
    if (filterPath != null) {
      if (!DFSUtil.isValidName(filterPath)) {
        throw new IOException("invalid path name '" + filterPath + "'");
      }
    }
    ArrayList<PathBasedCacheEntry> replies =
        new ArrayList<PathBasedCacheEntry>(NUM_PRE_ALLOCATED_ENTRIES);
    int numReplies = 0;
    SortedMap<Long, PathBasedCacheEntry> tailMap = entriesById.tailMap(prevId + 1);
    for (Entry<Long, PathBasedCacheEntry> cur : tailMap.entrySet()) {
      if (numReplies >= maxListCacheDirectivesResponses) {
        return new BatchedListEntries<PathBasedCacheEntry>(replies, true);
      }
      PathBasedCacheEntry curEntry = cur.getValue();
      PathBasedCacheDirective directive = cur.getValue().getDirective();
      if (filterPool != null && 
          !directive.getPool().equals(filterPool)) {
        continue;
      }
      if (filterPath != null &&
          !directive.getPath().equals(filterPath)) {
        continue;
      }
      CachePool pool = cachePools.get(curEntry.getDirective().getPool());
      if (pool == null) {
        LOG.error("invalid pool for PathBasedCacheEntry " + curEntry);
        continue;
      }
      if (pc.checkPermission(pool, FsAction.READ)) {
        replies.add(cur.getValue());
        numReplies++;
      }
    }
    return new BatchedListEntries<PathBasedCacheEntry>(replies, false);
  }

  /**
   * Create a cache pool.
   * 
   * Only the superuser should be able to call this function.
   *
   * @param info
   *          The info for the cache pool to create.
   */
  public synchronized void addCachePool(CachePoolInfo info)
      throws IOException {
    String poolName = info.getPoolName();
    CachePool.validateName(poolName);
    CachePool pool = cachePools.get(poolName);
    if (pool != null) {
      throw new IOException("cache pool " + poolName + " already exists.");
    }
    CachePool cachePool = new CachePool(poolName,
      info.getOwnerName(), info.getGroupName(), info.getMode(),
      info.getWeight());
    cachePools.put(poolName, cachePool);
    LOG.info("created new cache pool " + cachePool);
  }

  /**
   * Modify a cache pool.
   * 
   * Only the superuser should be able to call this function.
   *
   * @param info
   *          The info for the cache pool to modify.
   */
  public synchronized void modifyCachePool(CachePoolInfo info)
      throws IOException {
    String poolName = info.getPoolName();
    if (poolName.isEmpty()) {
      throw new IOException("invalid empty cache pool name");
    }
    CachePool pool = cachePools.get(poolName);
    if (pool == null) {
      throw new IOException("cache pool " + poolName + " does not exist.");
    }
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    if (info.getOwnerName() != null) {
      pool.setOwnerName(info.getOwnerName());
      bld.append(prefix).
        append("set owner to ").append(info.getOwnerName());
      prefix = "; ";
    }
    if (info.getGroupName() != null) {
      pool.setGroupName(info.getGroupName());
      bld.append(prefix).
        append("set group to ").append(info.getGroupName());
      prefix = "; ";
    }
    if (info.getMode() != null) {
      pool.setMode(info.getMode());
      bld.append(prefix).append("set mode to " + info.getMode());
      prefix = "; ";
    }
    if (info.getWeight() != null) {
      pool.setWeight(info.getWeight());
      bld.append(prefix).
        append("set weight to ").append(info.getWeight());
      prefix = "; ";
    }
    if (prefix.isEmpty()) {
      bld.append("no changes.");
    }
    LOG.info("modified " + poolName + "; " + bld.toString());
  }

  /**
   * Remove a cache pool.
   * 
   * Only the superuser should be able to call this function.
   *
   * @param poolName
   *          The name for the cache pool to remove.
   */
  public synchronized void removeCachePool(String poolName)
      throws IOException {
    CachePool pool = cachePools.remove(poolName);
    if (pool == null) {
      throw new IOException("can't remove nonexistent cache pool " + poolName);
    }
    
    // Remove entries using this pool
    // TODO: could optimize this somewhat to avoid the need to iterate
    // over all entries in entriesByDirective
    Iterator<Entry<PathBasedCacheDirective, PathBasedCacheEntry>> iter = 
        entriesByDirective.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<PathBasedCacheDirective, PathBasedCacheEntry> entry = iter.next();
      if (entry.getKey().getPool().equals(poolName)) {
        entriesById.remove(entry.getValue().getEntryId());
        iter.remove();
      }
    }
  }

  public synchronized BatchedListEntries<CachePoolInfo>
      listCachePools(FSPermissionChecker pc, String prevKey) {
    final int NUM_PRE_ALLOCATED_ENTRIES = 16;
    ArrayList<CachePoolInfo> results = 
        new ArrayList<CachePoolInfo>(NUM_PRE_ALLOCATED_ENTRIES);
    SortedMap<String, CachePool> tailMap = cachePools.tailMap(prevKey, false);
    int numListed = 0;
    for (Entry<String, CachePool> cur : tailMap.entrySet()) {
      if (numListed++ >= maxListCachePoolsResponses) {
        return new BatchedListEntries<CachePoolInfo>(results, true);
      }
      if (pc == null) {
        results.add(cur.getValue().getInfo(true));
      } else {
        results.add(cur.getValue().getInfo(pc));
      }
    }
    return new BatchedListEntries<CachePoolInfo>(results, false);
  }
}
