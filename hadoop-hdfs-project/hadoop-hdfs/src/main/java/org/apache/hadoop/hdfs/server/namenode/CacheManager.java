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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DESCRIPTORS_NUM_RESPONSES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DESCRIPTORS_NUM_RESPONSES_DEFAULT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDirective;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDescriptor;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.InvalidPoolNameError;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.UnexpectedAddPathBasedCacheDirectiveException;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.PoolWritePermissionDeniedError;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheEntry;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheDescriptorException.InvalidIdException;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheDescriptorException.NoSuchIdException;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheDescriptorException.UnexpectedRemovePathBasedCacheDescriptorException;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheDescriptorException.RemovePermissionDeniedException;

/**
 * The Cache Manager handles caching on DataNodes.
 */
public final class CacheManager {
  public static final Log LOG = LogFactory.getLog(CacheManager.class);

  /**
   * Cache entries, sorted by ID.
   *
   * listPathBasedCacheDescriptors relies on the ordering of elements in this map 
   * to track what has already been listed by the client.
   */
  private final TreeMap<Long, PathBasedCacheEntry> entriesById =
      new TreeMap<Long, PathBasedCacheEntry>();

  /**
   * Cache entries, sorted by path
   */
  private final TreeMap<String, List<PathBasedCacheEntry>> entriesByPath =
      new TreeMap<String, List<PathBasedCacheEntry>>();

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
  private final int maxListCacheDescriptorsResponses;

  final private FSNamesystem namesystem;
  final private FSDirectory dir;

  CacheManager(FSNamesystem namesystem, FSDirectory dir, Configuration conf) {
    // TODO: support loading and storing of the CacheManager state
    clear();
    this.namesystem = namesystem;
    this.dir = dir;
    maxListCachePoolsResponses = conf.getInt(
        DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES,
        DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT);
    maxListCacheDescriptorsResponses = conf.getInt(
        DFS_NAMENODE_LIST_CACHE_DESCRIPTORS_NUM_RESPONSES,
        DFS_NAMENODE_LIST_CACHE_DESCRIPTORS_NUM_RESPONSES_DEFAULT);
  }

  synchronized void clear() {
    entriesById.clear();
    entriesByPath.clear();
    cachePools.clear();
    nextEntryId = 1;
  }

  synchronized long getNextEntryId() throws IOException {
    if (nextEntryId == Long.MAX_VALUE) {
      throw new IOException("no more available IDs");
    }
    return nextEntryId++;
  }

  private synchronized PathBasedCacheEntry
      findEntry(PathBasedCacheDirective directive) {
    List<PathBasedCacheEntry> existing =
        entriesByPath.get(directive.getPath());
    if (existing == null) {
      return null;
    }
    for (PathBasedCacheEntry entry : existing) {
      if (entry.getPool().getName().equals(directive.getPool())) {
        return entry;
      }
    }
    return null;
  }

  public synchronized PathBasedCacheDescriptor addDirective(
      PathBasedCacheDirective directive, FSPermissionChecker pc)
      throws IOException {
    CachePool pool = cachePools.get(directive.getPool());
    if (pool == null) {
      LOG.info("addDirective " + directive + ": pool not found.");
      throw new InvalidPoolNameError(directive);
    }
    if ((pc != null) && (!pc.checkPermission(pool, FsAction.WRITE))) {
      LOG.info("addDirective " + directive + ": write permission denied.");
      throw new PoolWritePermissionDeniedError(directive);
    }
    try {
      directive.validate();
    } catch (IOException ioe) {
      LOG.info("addDirective " + directive + ": validation failed: "
          + ioe.getClass().getName() + ": " + ioe.getMessage());
      throw ioe;
    }
    
    // Check if we already have this entry.
    PathBasedCacheEntry existing = findEntry(directive);
    if (existing != null) {
      LOG.info("addDirective " + directive + ": there is an " +
          "existing directive " + existing + " in this pool.");
      return existing.getDescriptor();
    }
    // Add a new entry with the next available ID.
    PathBasedCacheEntry entry;
    try {
      entry = new PathBasedCacheEntry(getNextEntryId(),
          directive.getPath(), pool);
    } catch (IOException ioe) {
      throw new UnexpectedAddPathBasedCacheDirectiveException(directive);
    }
    LOG.info("addDirective " + directive + ": added cache directive "
        + directive);

    // Success!
    // First, add it to the various maps
    entriesById.put(entry.getEntryId(), entry);
    String path = directive.getPath();
    List<PathBasedCacheEntry> entryList = entriesByPath.get(path);
    if (entryList == null) {
      entryList = new ArrayList<PathBasedCacheEntry>(1);
      entriesByPath.put(path, entryList);
    }
    entryList.add(entry);

    // Next, set the path as cached in the namesystem
    try {
      INode node = dir.getINode(directive.getPath());
      if (node != null && node.isFile()) {
        INodeFile file = node.asFile();
        // TODO: adjustable cache replication factor
        namesystem.setCacheReplicationInt(directive.getPath(),
            file.getBlockReplication());
      } else {
        LOG.warn("Path " + directive.getPath() + " is not a file");
      }
    } catch (IOException ioe) {
      LOG.info("addDirective " + directive +": failed to cache file: " +
          ioe.getClass().getName() +": " + ioe.getMessage());
      throw ioe;
    }
    return entry.getDescriptor();
  }

  public synchronized void removeDescriptor(long id, FSPermissionChecker pc)
      throws IOException {
    // Check for invalid IDs.
    if (id <= 0) {
      LOG.info("removeDescriptor " + id + ": invalid non-positive " +
          "descriptor ID.");
      throw new InvalidIdException(id);
    }
    // Find the entry.
    PathBasedCacheEntry existing = entriesById.get(id);
    if (existing == null) {
      LOG.info("removeDescriptor " + id + ": entry not found.");
      throw new NoSuchIdException(id);
    }
    CachePool pool = cachePools.get(existing.getDescriptor().getPool());
    if (pool == null) {
      LOG.info("removeDescriptor " + id + ": pool not found for directive " +
        existing.getDescriptor());
      throw new UnexpectedRemovePathBasedCacheDescriptorException(id);
    }
    if ((pc != null) && (!pc.checkPermission(pool, FsAction.WRITE))) {
      LOG.info("removeDescriptor " + id + ": write permission denied to " +
          "pool " + pool + " for entry " + existing);
      throw new RemovePermissionDeniedException(id);
    }
    
    // Remove the corresponding entry in entriesByPath.
    String path = existing.getDescriptor().getPath();
    List<PathBasedCacheEntry> entries = entriesByPath.get(path);
    if (entries == null || !entries.remove(existing)) {
      throw new UnexpectedRemovePathBasedCacheDescriptorException(id);
    }
    if (entries.size() == 0) {
      entriesByPath.remove(path);
    }
    entriesById.remove(id);

    // Set the path as uncached in the namesystem
    try {
      INode node = dir.getINode(existing.getDescriptor().getPath());
      if (node != null && node.isFile()) {
        namesystem.setCacheReplicationInt(existing.getDescriptor().getPath(),
            (short) 0);
      }
    } catch (IOException e) {
      LOG.warn("removeDescriptor " + id + ": failure while setting cache"
          + " replication factor", e);
      throw e;
    }
    LOG.info("removeDescriptor successful for PathCacheEntry id " + id);
  }

  public synchronized BatchedListEntries<PathBasedCacheDescriptor> 
        listPathBasedCacheDescriptors(long prevId, String filterPool,
            String filterPath, FSPermissionChecker pc) throws IOException {
    final int NUM_PRE_ALLOCATED_ENTRIES = 16;
    if (filterPath != null) {
      if (!DFSUtil.isValidName(filterPath)) {
        throw new IOException("invalid path name '" + filterPath + "'");
      }
    }
    ArrayList<PathBasedCacheDescriptor> replies =
        new ArrayList<PathBasedCacheDescriptor>(NUM_PRE_ALLOCATED_ENTRIES);
    int numReplies = 0;
    SortedMap<Long, PathBasedCacheEntry> tailMap = entriesById.tailMap(prevId + 1);
    for (Entry<Long, PathBasedCacheEntry> cur : tailMap.entrySet()) {
      if (numReplies >= maxListCacheDescriptorsResponses) {
        return new BatchedListEntries<PathBasedCacheDescriptor>(replies, true);
      }
      PathBasedCacheEntry curEntry = cur.getValue();
      PathBasedCacheDirective directive = cur.getValue().getDescriptor();
      if (filterPool != null && 
          !directive.getPool().equals(filterPool)) {
        continue;
      }
      if (filterPath != null &&
          !directive.getPath().equals(filterPath)) {
        continue;
      }
      if (pc.checkPermission(curEntry.getPool(), FsAction.READ)) {
        replies.add(cur.getValue().getDescriptor());
        numReplies++;
      }
    }
    return new BatchedListEntries<PathBasedCacheDescriptor>(replies, false);
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
    CachePoolInfo.validate(info);
    String poolName = info.getPoolName();
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
    CachePoolInfo.validate(info);
    String poolName = info.getPoolName();
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
    CachePoolInfo.validateName(poolName);
    CachePool pool = cachePools.remove(poolName);
    if (pool == null) {
      throw new IOException("can't remove non-existent cache pool " + poolName);
    }
    
    // Remove entries using this pool
    // TODO: could optimize this somewhat to avoid the need to iterate
    // over all entries in entriesById
    Iterator<Entry<Long, PathBasedCacheEntry>> iter = 
        entriesById.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Long, PathBasedCacheEntry> entry = iter.next();
      if (entry.getValue().getPool() == pool) {
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
