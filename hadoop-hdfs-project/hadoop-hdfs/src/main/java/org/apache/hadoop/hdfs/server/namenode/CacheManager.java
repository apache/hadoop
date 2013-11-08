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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CACHING_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CACHING_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS_DEFAULT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.IdNotFoundException;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDirective;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheEntry;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.CacheReplicationMonitor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList.Type;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;

/**
 * The Cache Manager handles caching on DataNodes.
 *
 * This class is instantiated by the FSNamesystem when caching is enabled.
 * It maintains the mapping of cached blocks to datanodes via processing
 * datanode cache reports. Based on these reports and addition and removal of
 * caching directives, we will schedule caching and uncaching work.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public final class CacheManager {
  public static final Log LOG = LogFactory.getLog(CacheManager.class);

  // TODO: add pending / underCached / schedule cached blocks stats.

  /**
   * The FSNamesystem that contains this CacheManager.
   */
  private final FSNamesystem namesystem;

  /**
   * The BlockManager associated with the FSN that owns this CacheManager.
   */
  private final BlockManager blockManager;

  /**
   * Cache entries, sorted by ID.
   *
   * listPathBasedCacheDirectives relies on the ordering of elements in this map
   * to track what has already been listed by the client.
   */
  private final TreeMap<Long, PathBasedCacheEntry> entriesById =
      new TreeMap<Long, PathBasedCacheEntry>();

  /**
   * The entry ID to use for a new entry.  Entry IDs always increase, and are
   * never reused.
   */
  private long nextEntryId;

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
   * Maximum number of cache pools to list in one operation.
   */
  private final int maxListCachePoolsResponses;

  /**
   * Maximum number of cache pool directives to list in one operation.
   */
  private final int maxListCacheDirectivesNumResponses;

  /**
   * Interval between scans in milliseconds.
   */
  private final long scanIntervalMs;

  /**
   * Whether caching is enabled.
   *
   * If caching is disabled, we will not process cache reports or store
   * information about what is cached where.  We also do not start the
   * CacheReplicationMonitor thread.  This will save resources, but provide
   * less functionality.
   *     
   * Even when caching is disabled, we still store path-based cache
   * information.  This information is stored in the edit log and fsimage.  We
   * don't want to lose it just because a configuration setting was turned off.
   * However, we will not act on this information if caching is disabled.
   */
  private final boolean enabled;

  /**
   * Whether the CacheManager is active.
   * 
   * When the CacheManager is active, it tells the DataNodes what to cache
   * and uncache.  The CacheManager cannot become active if enabled = false.
   */
  private boolean active = false;

  /**
   * All cached blocks.
   */
  private final GSet<CachedBlock, CachedBlock> cachedBlocks;

  /**
   * The CacheReplicationMonitor.
   */
  private CacheReplicationMonitor monitor;

  CacheManager(FSNamesystem namesystem, Configuration conf,
      BlockManager blockManager) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    this.nextEntryId = 1;
    this.maxListCachePoolsResponses = conf.getInt(
        DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES,
        DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT);
    this.maxListCacheDirectivesNumResponses = conf.getInt(
        DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES,
        DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES_DEFAULT);
    scanIntervalMs = conf.getLong(
        DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS,
        DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS_DEFAULT);
    this.enabled = conf.getBoolean(DFS_NAMENODE_CACHING_ENABLED_KEY,
        DFS_NAMENODE_CACHING_ENABLED_DEFAULT);
    this.cachedBlocks = !enabled ? null :
        new LightWeightGSet<CachedBlock, CachedBlock>(
            LightWeightGSet.computeCapacity(0.25, "cachedBlocks"));
  }

  /**
   * Activate the cache manager.
   * 
   * When the cache manager is active, tell the datanodes where to cache files.
   */
  public void activate() {
    assert namesystem.hasWriteLock();
    if (enabled && (!active)) {
      LOG.info("Activating CacheManager.  " +
          "Starting replication monitor thread...");
      active = true;
      monitor = new CacheReplicationMonitor(namesystem, this,
         scanIntervalMs);
      monitor.start();
    }
  }

  /**
   * Deactivate the cache manager.
   * 
   * When the cache manager is inactive, it does not tell the datanodes where to
   * cache files.
   */
  public void deactivate() {
    assert namesystem.hasWriteLock();
    if (active) {
      LOG.info("Deactivating CacheManager.  " +
          "stopping CacheReplicationMonitor thread...");
      active = false;
      IOUtils.closeQuietly(monitor);
      monitor = null;
      LOG.info("CacheReplicationMonitor thread stopped and deactivated.");
    }
  }

  /**
   * Return true only if the cache manager is active.
   * Must be called under the FSN read or write lock.
   */
  public boolean isActive() {
    return active;
  }

  public TreeMap<Long, PathBasedCacheEntry> getEntriesById() {
    assert namesystem.hasReadOrWriteLock();
    return entriesById;
  }
  
  @VisibleForTesting
  public GSet<CachedBlock, CachedBlock> getCachedBlocks() {
    assert namesystem.hasReadOrWriteLock();
    return cachedBlocks;
  }

  private long getNextEntryId() throws IOException {
    assert namesystem.hasWriteLock();
    if (nextEntryId == Long.MAX_VALUE) {
      throw new IOException("No more available IDs");
    }
    return nextEntryId++;
  }

  private void addInternal(PathBasedCacheEntry entry) {
    entriesById.put(entry.getEntryId(), entry);
    String path = entry.getPath();
    List<PathBasedCacheEntry> entryList = entriesByPath.get(path);
    if (entryList == null) {
      entryList = new ArrayList<PathBasedCacheEntry>(1);
      entriesByPath.put(path, entryList);
    }
    entryList.add(entry);
  }

  public PathBasedCacheDirective addDirective(
      PathBasedCacheDirective directive, FSPermissionChecker pc)
      throws IOException {
    assert namesystem.hasWriteLock();
    PathBasedCacheEntry entry;
    try {
      if (directive.getPool() == null) {
        throw new IdNotFoundException("addDirective: no pool was specified.");
      }
      if (directive.getPool().isEmpty()) {
        throw new IdNotFoundException("addDirective: pool name was empty.");
      }
      CachePool pool = cachePools.get(directive.getPool());
      if (pool == null) {
        throw new IdNotFoundException("addDirective: no such pool as " +
            directive.getPool());
      }
      if ((pc != null) && (!pc.checkPermission(pool, FsAction.WRITE))) {
        throw new AccessControlException("addDirective: write " +
            "permission denied for pool " + directive.getPool());
      }
      if (directive.getPath() == null) {
        throw new IOException("addDirective: no path was specified.");
      }
      String path = directive.getPath().toUri().getPath();
      if (!DFSUtil.isValidName(path)) {
        throw new IOException("addDirective: path '" + path + "' is invalid.");
      }
      short replication = directive.getReplication() == null ? 
          (short)1 : directive.getReplication();
      if (replication <= 0) {
        throw new IOException("addDirective: replication " + replication +
            " is invalid.");
      }
      long id;
      if (directive.getId() != null) {
        // We are loading an entry from the edit log.
        // Use the ID from the edit log.
        id = directive.getId();
      } else {
        // Add a new entry with the next available ID.
        id = getNextEntryId();
      }
      entry = new PathBasedCacheEntry(id, path, replication, pool);
      addInternal(entry);
    } catch (IOException e) {
      LOG.warn("addDirective " + directive + ": failed.", e);
      throw e;
    }
    LOG.info("addDirective " + directive + ": succeeded.");
    if (monitor != null) {
      monitor.kick();
    }
    return entry.toDirective();
  }

  public void modifyDirective(PathBasedCacheDirective directive,
      FSPermissionChecker pc) throws IOException {
    assert namesystem.hasWriteLock();
    String idString =
        (directive.getId() == null) ?
            "(null)" : directive.getId().toString();
    try {
      // Check for invalid IDs.
      Long id = directive.getId();
      if (id == null) {
        throw new IdNotFoundException("modifyDirective: " +
            "no ID to modify was supplied.");
      }
      if (id <= 0) {
        throw new IdNotFoundException("modifyDirective " + id +
            ": invalid non-positive directive ID.");
      }
      // Find the entry.
      PathBasedCacheEntry prevEntry = entriesById.get(id);
      if (prevEntry == null) {
        throw new IdNotFoundException("modifyDirective " + id +
            ": id not found.");
      }
      if ((pc != null) &&
          (!pc.checkPermission(prevEntry.getPool(), FsAction.WRITE))) {
        throw new AccessControlException("modifyDirective " + id +
            ": permission denied for initial pool " + prevEntry.getPool());
      }
      String path = prevEntry.getPath();
      if (directive.getPath() != null) {
        path = directive.getPath().toUri().getPath();
        if (!DFSUtil.isValidName(path)) {
          throw new IOException("modifyDirective " + id + ": new path " +
              path + " is not valid.");
        }
      }
      short replication = (directive.getReplication() != null) ?
          directive.getReplication() : prevEntry.getReplication();
      if (replication <= 0) {
        throw new IOException("modifyDirective: replication " + replication +
            " is invalid.");
      }
      CachePool pool = prevEntry.getPool();
      if (directive.getPool() != null) {
        pool = cachePools.get(directive.getPool());
        if (pool == null) {
          throw new IdNotFoundException("modifyDirective " + id +
              ": pool " + directive.getPool() + " not found.");
        }
        if (directive.getPool().isEmpty()) {
          throw new IdNotFoundException("modifyDirective: pool name was " +
              "empty.");
        }
        if ((pc != null) &&
            (!pc.checkPermission(pool, FsAction.WRITE))) {
          throw new AccessControlException("modifyDirective " + id +
              ": permission denied for target pool " + pool);
        }
      }
      removeInternal(prevEntry);
      PathBasedCacheEntry newEntry =
          new PathBasedCacheEntry(id, path, replication, pool);
      addInternal(newEntry);
    } catch (IOException e) {
      LOG.warn("modifyDirective " + idString + ": failed.", e);
      throw e;
    }
    LOG.info("modifyDirective " + idString + ": successfully applied " +
        directive);
  }

  public void removeInternal(PathBasedCacheEntry existing)
      throws IOException {
    assert namesystem.hasWriteLock();
    // Remove the corresponding entry in entriesByPath.
    String path = existing.getPath();
    List<PathBasedCacheEntry> entries = entriesByPath.get(path);
    if (entries == null || !entries.remove(existing)) {
      throw new IdNotFoundException("removeInternal: failed to locate entry " +
          existing.getEntryId() + " by path " + existing.getPath());
    }
    if (entries.size() == 0) {
      entriesByPath.remove(path);
    }
    entriesById.remove(existing.getEntryId());
  }

  public void removeDirective(long id, FSPermissionChecker pc)
      throws IOException {
    assert namesystem.hasWriteLock();
    try {
      // Check for invalid IDs.
      if (id <= 0) {
        throw new IdNotFoundException("removeDirective " + id + ": invalid " +
            "non-positive directive ID.");
      }
      // Find the entry.
      PathBasedCacheEntry existing = entriesById.get(id);
      if (existing == null) {
        throw new IdNotFoundException("removeDirective " + id +
            ": id not found.");
      }
      if ((pc != null) &&
          (!pc.checkPermission(existing.getPool(), FsAction.WRITE))) {
        throw new AccessControlException("removeDirective " + id +
            ": write permission denied on pool " +
            existing.getPool().getPoolName());
      }
      removeInternal(existing);
    } catch (IOException e) {
      LOG.warn("removeDirective " + id + " failed.", e);
      throw e;
    }
    if (monitor != null) {
      monitor.kick();
    }
    LOG.info("removeDirective " + id + ": succeeded.");
  }

  public BatchedListEntries<PathBasedCacheDirective> 
        listPathBasedCacheDirectives(long prevId,
            PathBasedCacheDirective filter,
            FSPermissionChecker pc) throws IOException {
    assert namesystem.hasReadOrWriteLock();
    final int NUM_PRE_ALLOCATED_ENTRIES = 16;
    String filterPath = null;
    if (filter.getId() != null) {
      throw new IOException("we currently don't support filtering by ID");
    }
    if (filter.getPath() != null) {
      filterPath = filter.getPath().toUri().getPath();
      if (!DFSUtil.isValidName(filterPath)) {
        throw new IOException("listPathBasedCacheDirectives: invalid " +
            "path name '" + filterPath + "'");
      }
    }
    if (filter.getReplication() != null) {
      throw new IOException("we currently don't support filtering " +
          "by replication");
    }
    ArrayList<PathBasedCacheDirective> replies =
        new ArrayList<PathBasedCacheDirective>(NUM_PRE_ALLOCATED_ENTRIES);
    int numReplies = 0;
    SortedMap<Long, PathBasedCacheEntry> tailMap =
      entriesById.tailMap(prevId + 1);
    for (Entry<Long, PathBasedCacheEntry> cur : tailMap.entrySet()) {
      if (numReplies >= maxListCacheDirectivesNumResponses) {
        return new BatchedListEntries<PathBasedCacheDirective>(replies, true);
      }
      PathBasedCacheEntry curEntry = cur.getValue();
      PathBasedCacheDirective directive = cur.getValue().toDirective();
      if (filter.getPool() != null && 
          !directive.getPool().equals(filter.getPool())) {
        continue;
      }
      if (filterPath != null &&
          !directive.getPath().toUri().getPath().equals(filterPath)) {
        continue;
      }
      if ((pc == null) ||
          (pc.checkPermission(curEntry.getPool(), FsAction.READ))) {
        replies.add(cur.getValue().toDirective());
        numReplies++;
      }
    }
    return new BatchedListEntries<PathBasedCacheDirective>(replies, false);
  }

  /**
   * Create a cache pool.
   * 
   * Only the superuser should be able to call this function.
   *
   * @param info    The info for the cache pool to create.
   * @return        Information about the cache pool we created.
   */
  public CachePoolInfo addCachePool(CachePoolInfo info)
      throws IOException {
    assert namesystem.hasWriteLock();
    CachePoolInfo.validate(info);
    String poolName = info.getPoolName();
    CachePool pool = cachePools.get(poolName);
    if (pool != null) {
      throw new IOException("cache pool " + poolName + " already exists.");
    }
    pool = CachePool.createFromInfoAndDefaults(info);
    cachePools.put(pool.getPoolName(), pool);
    LOG.info("created new cache pool " + pool);
    return pool.getInfo(true);
  }

  /**
   * Modify a cache pool.
   * 
   * Only the superuser should be able to call this function.
   *
   * @param info
   *          The info for the cache pool to modify.
   */
  public void modifyCachePool(CachePoolInfo info)
      throws IOException {
    assert namesystem.hasWriteLock();
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
  public void removeCachePool(String poolName)
      throws IOException {
    assert namesystem.hasWriteLock();
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
        entriesByPath.remove(entry.getValue().getPath());
        iter.remove();
      }
    }
    if (monitor != null) {
      monitor.kick();
    }
  }

  public BatchedListEntries<CachePoolInfo>
      listCachePools(FSPermissionChecker pc, String prevKey) {
    assert namesystem.hasReadOrWriteLock();
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

  public void setCachedLocations(LocatedBlock block) {
    if (!enabled) {
      return;
    }
    CachedBlock cachedBlock =
        new CachedBlock(block.getBlock().getBlockId(),
            (short)0, false);
    cachedBlock = cachedBlocks.get(cachedBlock);
    if (cachedBlock == null) {
      return;
    }
    List<DatanodeDescriptor> datanodes = cachedBlock.getDatanodes(Type.CACHED);
    for (DatanodeDescriptor datanode : datanodes) {
      block.addCachedLoc(datanode);
    }
  }

  public final void processCacheReport(final DatanodeID datanodeID,
      final List<Long> blockIds) throws IOException {
    if (!enabled) {
      LOG.info("Ignoring cache report from " + datanodeID +
          " because " + DFS_NAMENODE_CACHING_ENABLED_KEY + " = false. " +
          "number of blocks: " + blockIds.size());
      return;
    }
    namesystem.writeLock();
    final long startTime = Time.monotonicNow();
    final long endTime;
    try {
      final DatanodeDescriptor datanode = 
          blockManager.getDatanodeManager().getDatanode(datanodeID);
      if (datanode == null || !datanode.isAlive) {
        throw new IOException(
            "processCacheReport from dead or unregistered datanode: " +
            datanode);
      }
      processCacheReportImpl(datanode, blockIds);
    } finally {
      endTime = Time.monotonicNow();
      namesystem.writeUnlock();
    }

    // Log the block report processing stats from Namenode perspective
    final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
    if (metrics != null) {
      metrics.addCacheBlockReport((int) (endTime - startTime));
    }
    LOG.info("Processed cache report from "
        + datanodeID + ", blocks: " + blockIds.size()
        + ", processing time: " + (endTime - startTime) + " msecs");
  }

  private void processCacheReportImpl(final DatanodeDescriptor datanode,
      final List<Long> blockIds) {
    CachedBlocksList cached = datanode.getCached();
    cached.clear();
    for (Iterator<Long> iter = blockIds.iterator(); iter.hasNext(); ) {
      Block block = new Block(iter.next());
      BlockInfo blockInfo = blockManager.getStoredBlock(block);
      if (blockInfo.getGenerationStamp() < block.getGenerationStamp()) {
        // The NameNode will eventually remove or update the out-of-date block.
        // Until then, we pretend that it isn't cached.
        LOG.warn("Genstamp in cache report disagrees with our genstamp for " +
          block + ": expected genstamp " + blockInfo.getGenerationStamp());
        continue;
      }
      if (!blockInfo.isComplete()) {
        LOG.warn("Ignoring block id " + block.getBlockId() + ", because " +
            "it is in not complete yet.  It is in state " + 
            blockInfo.getBlockUCState());
        continue;
      }
      Collection<DatanodeDescriptor> corruptReplicas =
          blockManager.getCorruptReplicas(blockInfo);
      if ((corruptReplicas != null) && corruptReplicas.contains(datanode)) {
        // The NameNode will eventually remove or update the corrupt block.
        // Until then, we pretend that it isn't cached.
        LOG.warn("Ignoring cached replica on " + datanode + " of " + block +
            " because it is corrupt.");
        continue;
      }
      CachedBlock cachedBlock =
          new CachedBlock(block.getBlockId(), (short)0, false);
      CachedBlock prevCachedBlock = cachedBlocks.get(cachedBlock);
      // Use the existing CachedBlock if it's present; otherwise,
      // insert a new one.
      if (prevCachedBlock != null) {
        cachedBlock = prevCachedBlock;
      } else {
        cachedBlocks.put(cachedBlock);
      }
      if (!cachedBlock.isPresent(datanode.getCached())) {
        datanode.getCached().add(cachedBlock);
      }
      if (cachedBlock.isPresent(datanode.getPendingCached())) {
        datanode.getPendingCached().remove(cachedBlock);
      }
    }
  }

  /**
   * Saves the current state of the CacheManager to the DataOutput. Used
   * to persist CacheManager state in the FSImage.
   * @param out DataOutput to persist state
   * @param sdPath path of the storage directory
   * @throws IOException
   */
  public void saveState(DataOutput out, String sdPath)
      throws IOException {
    out.writeLong(nextEntryId);
    savePools(out, sdPath);
    saveEntries(out, sdPath);
  }

  /**
   * Reloads CacheManager state from the passed DataInput. Used during namenode
   * startup to restore CacheManager state from an FSImage.
   * @param in DataInput from which to restore state
   * @throws IOException
   */
  public void loadState(DataInput in) throws IOException {
    nextEntryId = in.readLong();
    // pools need to be loaded first since entries point to their parent pool
    loadPools(in);
    loadEntries(in);
  }

  /**
   * Save cache pools to fsimage
   */
  private void savePools(DataOutput out,
      String sdPath) throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.CACHE_POOLS, sdPath);
    prog.beginStep(Phase.SAVING_CHECKPOINT, step);
    prog.setTotal(Phase.SAVING_CHECKPOINT, step, cachePools.size());
    Counter counter = prog.getCounter(Phase.SAVING_CHECKPOINT, step);
    out.writeInt(cachePools.size());
    for (CachePool pool: cachePools.values()) {
      pool.getInfo(true).writeTo(out);
      counter.increment();
    }
    prog.endStep(Phase.SAVING_CHECKPOINT, step);
  }

  /*
   * Save cache entries to fsimage
   */
  private void saveEntries(DataOutput out, String sdPath)
      throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.CACHE_ENTRIES, sdPath);
    prog.beginStep(Phase.SAVING_CHECKPOINT, step);
    prog.setTotal(Phase.SAVING_CHECKPOINT, step, entriesById.size());
    Counter counter = prog.getCounter(Phase.SAVING_CHECKPOINT, step);
    out.writeInt(entriesById.size());
    for (PathBasedCacheEntry entry: entriesById.values()) {
      out.writeLong(entry.getEntryId());
      Text.writeString(out, entry.getPath());
      out.writeShort(entry.getReplication());
      Text.writeString(out, entry.getPool().getPoolName());
      counter.increment();
    }
    prog.endStep(Phase.SAVING_CHECKPOINT, step);
  }

  /**
   * Load cache pools from fsimage
   */
  private void loadPools(DataInput in)
      throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.CACHE_POOLS);
    prog.beginStep(Phase.LOADING_FSIMAGE, step);
    int numberOfPools = in.readInt();
    prog.setTotal(Phase.LOADING_FSIMAGE, step, numberOfPools);
    Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, step);
    for (int i = 0; i < numberOfPools; i++) {
      addCachePool(CachePoolInfo.readFrom(in));
      counter.increment();
    }
    prog.endStep(Phase.LOADING_FSIMAGE, step);
  }

  /**
   * Load cache entries from the fsimage
   */
  private void loadEntries(DataInput in) throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.CACHE_ENTRIES);
    prog.beginStep(Phase.LOADING_FSIMAGE, step);
    int numberOfEntries = in.readInt();
    prog.setTotal(Phase.LOADING_FSIMAGE, step, numberOfEntries);
    Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, step);
    for (int i = 0; i < numberOfEntries; i++) {
      long entryId = in.readLong();
      String path = Text.readString(in);
      short replication = in.readShort();
      String poolName = Text.readString(in);
      // Get pool reference by looking it up in the map
      CachePool pool = cachePools.get(poolName);
      if (pool == null) {
        throw new IOException("Entry refers to pool " + poolName +
            ", which does not exist.");
      }
      PathBasedCacheEntry entry =
          new PathBasedCacheEntry(entryId, path, replication, pool);
      if (entriesById.put(entry.getEntryId(), entry) != null) {
        throw new IOException("An entry with ID " + entry.getEntryId() +
            " already exists");
      }
      List<PathBasedCacheEntry> entries = entriesByPath.get(entry.getPath());
      if (entries == null) {
        entries = new LinkedList<PathBasedCacheEntry>();
        entriesByPath.put(entry.getPath(), entries);
      }
      entries.add(entry);
      counter.increment();
    }
    prog.endStep(Phase.LOADING_FSIMAGE, step);
  }
}
