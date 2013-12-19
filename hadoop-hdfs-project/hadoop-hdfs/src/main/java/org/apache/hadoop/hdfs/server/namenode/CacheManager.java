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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CACHING_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CACHING_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS_DEFAULT;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
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
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveStats;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
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
import org.apache.hadoop.hdfs.util.ReadOnlyList;
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
   * Cache directives, sorted by ID.
   *
   * listCacheDirectives relies on the ordering of elements in this map
   * to track what has already been listed by the client.
   */
  private final TreeMap<Long, CacheDirective> directivesById =
      new TreeMap<Long, CacheDirective>();

  /**
   * The directive ID to use for a new directive.  IDs always increase, and are
   * never reused.
   */
  private long nextDirectiveId;

  /**
   * Cache directives, sorted by path
   */
  private final TreeMap<String, List<CacheDirective>> directivesByPath =
      new TreeMap<String, List<CacheDirective>>();

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
    this.nextDirectiveId = 1;
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

  /**
   * @return Unmodifiable view of the collection of CachePools.
   */
  public Collection<CachePool> getCachePools() {
    assert namesystem.hasReadLock();
    return Collections.unmodifiableCollection(cachePools.values());
  }

  /**
   * @return Unmodifiable view of the collection of CacheDirectives.
   */
  public Collection<CacheDirective> getCacheDirectives() {
    assert namesystem.hasReadLock();
    return Collections.unmodifiableCollection(directivesById.values());
  }
  
  @VisibleForTesting
  public GSet<CachedBlock, CachedBlock> getCachedBlocks() {
    assert namesystem.hasReadLock();
    return cachedBlocks;
  }

  private long getNextDirectiveId() throws IOException {
    assert namesystem.hasWriteLock();
    if (nextDirectiveId >= Long.MAX_VALUE - 1) {
      throw new IOException("No more available IDs.");
    }
    return nextDirectiveId++;
  }

  // Helper getter / validation methods

  private static void checkWritePermission(FSPermissionChecker pc,
      CachePool pool) throws AccessControlException {
    if ((pc != null)) {
      pc.checkPermission(pool, FsAction.WRITE);
    }
  }

  private static String validatePoolName(CacheDirectiveInfo directive)
      throws InvalidRequestException {
    String pool = directive.getPool();
    if (pool == null) {
      throw new InvalidRequestException("No pool specified.");
    }
    if (pool.isEmpty()) {
      throw new InvalidRequestException("Invalid empty pool name.");
    }
    return pool;
  }

  private static String validatePath(CacheDirectiveInfo directive)
      throws InvalidRequestException {
    if (directive.getPath() == null) {
      throw new InvalidRequestException("No path specified.");
    }
    String path = directive.getPath().toUri().getPath();
    if (!DFSUtil.isValidName(path)) {
      throw new InvalidRequestException("Invalid path '" + path + "'.");
    }
    return path;
  }

  private static short validateReplication(CacheDirectiveInfo directive,
      short defaultValue) throws InvalidRequestException {
    short repl = (directive.getReplication() != null)
        ? directive.getReplication() : defaultValue;
    if (repl <= 0) {
      throw new InvalidRequestException("Invalid replication factor " + repl
          + " <= 0");
    }
    return repl;
  }

  /**
   * Calculates the absolute expiry time of the directive from the
   * {@link CacheDirectiveInfo.Expiration}. This converts a relative Expiration
   * into an absolute time based on the local clock.
   * 
   * @param directive from which to get the expiry time
   * @param defaultValue to use if Expiration is not set
   * @return Absolute expiry time in milliseconds since Unix epoch
   * @throws InvalidRequestException if the Expiration is invalid
   */
  private static long validateExpiryTime(CacheDirectiveInfo directive,
      long defaultValue) throws InvalidRequestException {
    long expiryTime;
    CacheDirectiveInfo.Expiration expiration = directive.getExpiration();
    if (expiration != null) {
      if (expiration.getMillis() < 0) {
        throw new InvalidRequestException("Cannot set a negative expiration: "
            + expiration.getMillis());
      }
      // Converts a relative duration into an absolute time based on the local
      // clock
      expiryTime = expiration.getAbsoluteMillis();
    } else {
      expiryTime = defaultValue;
    }
    return expiryTime;
  }

  /**
   * Throws an exception if the CachePool does not have enough capacity to
   * cache the given path at the replication factor.
   *
   * @param pool CachePool where the path is being cached
   * @param path Path that is being cached
   * @param replication Replication factor of the path
   * @throws InvalidRequestException if the pool does not have enough capacity
   */
  private void checkLimit(CachePool pool, String path,
      short replication) throws InvalidRequestException {
    CacheDirectiveStats stats = computeNeeded(path, replication);
    if (pool.getBytesNeeded() + (stats.getBytesNeeded() * replication) > pool
        .getLimit()) {
      throw new InvalidRequestException("Caching path " + path + " of size "
          + stats.getBytesNeeded() / replication + " bytes at replication "
          + replication + " would exceed pool " + pool.getPoolName()
          + "'s remaining capacity of "
          + (pool.getLimit() - pool.getBytesNeeded()) + " bytes.");
    }
  }

  /**
   * Computes the needed number of bytes and files for a path.
   * @return CacheDirectiveStats describing the needed stats for this path
   */
  private CacheDirectiveStats computeNeeded(String path, short replication) {
    FSDirectory fsDir = namesystem.getFSDirectory();
    INode node;
    long requestedBytes = 0;
    long requestedFiles = 0;
    CacheDirectiveStats.Builder builder = new CacheDirectiveStats.Builder();
    try {
      node = fsDir.getINode(path);
    } catch (UnresolvedLinkException e) {
      // We don't cache through symlinks
      return builder.build();
    }
    if (node == null) {
      return builder.build();
    }
    if (node.isFile()) {
      requestedFiles = 1;
      INodeFile file = node.asFile();
      requestedBytes = file.computeFileSize();
    } else if (node.isDirectory()) {
      INodeDirectory dir = node.asDirectory();
      ReadOnlyList<INode> children = dir.getChildrenList(null);
      requestedFiles = children.size();
      for (INode child : children) {
        if (child.isFile()) {
          requestedBytes += child.asFile().computeFileSize();
        }
      }
    }
    return new CacheDirectiveStats.Builder()
        .setBytesNeeded(requestedBytes)
        .setFilesCached(requestedFiles)
        .build();
  }

  /**
   * Get a CacheDirective by ID, validating the ID and that the directive
   * exists.
   */
  private CacheDirective getById(long id) throws InvalidRequestException {
    // Check for invalid IDs.
    if (id <= 0) {
      throw new InvalidRequestException("Invalid negative ID.");
    }
    // Find the directive.
    CacheDirective directive = directivesById.get(id);
    if (directive == null) {
      throw new InvalidRequestException("No directive with ID " + id
          + " found.");
    }
    return directive;
  }

  /**
   * Get a CachePool by name, validating that it exists.
   */
  private CachePool getCachePool(String poolName)
      throws InvalidRequestException {
    CachePool pool = cachePools.get(poolName);
    if (pool == null) {
      throw new InvalidRequestException("Unknown pool " + poolName);
    }
    return pool;
  }

  // RPC handlers

  private void addInternal(CacheDirective directive, CachePool pool) {
    boolean addedDirective = pool.getDirectiveList().add(directive);
    assert addedDirective;
    directivesById.put(directive.getId(), directive);
    String path = directive.getPath();
    List<CacheDirective> directives = directivesByPath.get(path);
    if (directives == null) {
      directives = new ArrayList<CacheDirective>(1);
      directivesByPath.put(path, directives);
    }
    directives.add(directive);
    // Fix up pool stats
    CacheDirectiveStats stats =
        computeNeeded(directive.getPath(), directive.getReplication());
    directive.addBytesNeeded(stats.getBytesNeeded());
    directive.addFilesNeeded(directive.getFilesNeeded());

    if (monitor != null) {
      monitor.setNeedsRescan();
    }
  }

  /**
   * To be called only from the edit log loading code
   */
  CacheDirectiveInfo addDirectiveFromEditLog(CacheDirectiveInfo directive)
      throws InvalidRequestException {
    long id = directive.getId();
    CacheDirective entry =
        new CacheDirective(
            directive.getId(),
            directive.getPath().toUri().getPath(),
            directive.getReplication(),
            directive.getExpiration().getAbsoluteMillis());
    CachePool pool = cachePools.get(directive.getPool());
    addInternal(entry, pool);
    if (nextDirectiveId <= id) {
      nextDirectiveId = id + 1;
    }
    return entry.toInfo();
  }

  public CacheDirectiveInfo addDirective(
      CacheDirectiveInfo info, FSPermissionChecker pc, EnumSet<CacheFlag> flags)
      throws IOException {
    assert namesystem.hasWriteLock();
    CacheDirective directive;
    try {
      CachePool pool = getCachePool(validatePoolName(info));
      checkWritePermission(pc, pool);
      String path = validatePath(info);
      short replication = validateReplication(info, (short)1);
      long expiryTime = validateExpiryTime(info,
          CacheDirectiveInfo.Expiration.EXPIRY_NEVER);
      // Do quota validation if required
      if (!flags.contains(CacheFlag.FORCE)) {
        // Can't kick and wait if caching is disabled
        if (monitor != null) {
          monitor.waitForRescan();
        }
        checkLimit(pool, path, replication);
      }
      // All validation passed
      // Add a new entry with the next available ID.
      long id = getNextDirectiveId();
      directive = new CacheDirective(id, path, replication, expiryTime);
      addInternal(directive, pool);
    } catch (IOException e) {
      LOG.warn("addDirective of " + info + " failed: ", e);
      throw e;
    }
    LOG.info("addDirective of " + info + " successful.");
    return directive.toInfo();
  }

  public void modifyDirective(CacheDirectiveInfo info,
      FSPermissionChecker pc, EnumSet<CacheFlag> flags) throws IOException {
    assert namesystem.hasWriteLock();
    String idString =
        (info.getId() == null) ?
            "(null)" : info.getId().toString();
    try {
      // Check for invalid IDs.
      Long id = info.getId();
      if (id == null) {
        throw new InvalidRequestException("Must supply an ID.");
      }
      CacheDirective prevEntry = getById(id);
      checkWritePermission(pc, prevEntry.getPool());
      String path = prevEntry.getPath();
      if (info.getPath() != null) {
        path = validatePath(info);
      }

      short replication = prevEntry.getReplication();
      replication = validateReplication(info, replication);

      long expiryTime = prevEntry.getExpiryTime();
      expiryTime = validateExpiryTime(info, expiryTime);

      CachePool pool = prevEntry.getPool();
      if (info.getPool() != null) {
        pool = getCachePool(validatePoolName(info));
        checkWritePermission(pc, pool);
        if (!flags.contains(CacheFlag.FORCE)) {
          // Can't kick and wait if caching is disabled
          if (monitor != null) {
            monitor.waitForRescan();
          }
          checkLimit(pool, path, replication);
        }
      }
      removeInternal(prevEntry);
      CacheDirective newEntry =
          new CacheDirective(id, path, replication, expiryTime);
      addInternal(newEntry, pool);
    } catch (IOException e) {
      LOG.warn("modifyDirective of " + idString + " failed: ", e);
      throw e;
    }
    LOG.info("modifyDirective of " + idString + " successfully applied " +
        info+ ".");
  }

  public void removeInternal(CacheDirective directive)
      throws InvalidRequestException {
    assert namesystem.hasWriteLock();
    // Remove the corresponding entry in directivesByPath.
    String path = directive.getPath();
    List<CacheDirective> directives = directivesByPath.get(path);
    if (directives == null || !directives.remove(directive)) {
      throw new InvalidRequestException("Failed to locate entry " +
          directive.getId() + " by path " + directive.getPath());
    }
    if (directives.size() == 0) {
      directivesByPath.remove(path);
    }
    // Fix up the stats from removing the pool
    final CachePool pool = directive.getPool();
    directive.addBytesNeeded(-directive.getBytesNeeded());
    directive.addFilesNeeded(-directive.getFilesNeeded());

    directivesById.remove(directive.getId());
    pool.getDirectiveList().remove(directive);
    assert directive.getPool() == null;

    if (monitor != null) {
      monitor.setNeedsRescan();
    }
  }

  public void removeDirective(long id, FSPermissionChecker pc)
      throws IOException {
    assert namesystem.hasWriteLock();
    try {
      CacheDirective directive = getById(id);
      checkWritePermission(pc, directive.getPool());
      removeInternal(directive);
    } catch (IOException e) {
      LOG.warn("removeDirective of " + id + " failed: ", e);
      throw e;
    }
    LOG.info("removeDirective of " + id + " successful.");
  }

  public BatchedListEntries<CacheDirectiveEntry> 
        listCacheDirectives(long prevId,
            CacheDirectiveInfo filter,
            FSPermissionChecker pc) throws IOException {
    assert namesystem.hasReadLock();
    final int NUM_PRE_ALLOCATED_ENTRIES = 16;
    String filterPath = null;
    if (filter.getId() != null) {
      throw new IOException("Filtering by ID is unsupported.");
    }
    if (filter.getPath() != null) {
      filterPath = validatePath(filter);
    }
    if (filter.getReplication() != null) {
      throw new IOException("Filtering by replication is unsupported.");
    }
    if (monitor != null) {
      monitor.waitForRescanIfNeeded();
    }
    ArrayList<CacheDirectiveEntry> replies =
        new ArrayList<CacheDirectiveEntry>(NUM_PRE_ALLOCATED_ENTRIES);
    int numReplies = 0;
    SortedMap<Long, CacheDirective> tailMap =
      directivesById.tailMap(prevId + 1);
    for (Entry<Long, CacheDirective> cur : tailMap.entrySet()) {
      if (numReplies >= maxListCacheDirectivesNumResponses) {
        return new BatchedListEntries<CacheDirectiveEntry>(replies, true);
      }
      CacheDirective curDirective = cur.getValue();
      CacheDirectiveInfo info = cur.getValue().toInfo();
      if (filter.getPool() != null && 
          !info.getPool().equals(filter.getPool())) {
        continue;
      }
      if (filterPath != null &&
          !info.getPath().toUri().getPath().equals(filterPath)) {
        continue;
      }
      boolean hasPermission = true;
      if (pc != null) {
        try {
          pc.checkPermission(curDirective.getPool(), FsAction.READ);
        } catch (AccessControlException e) {
          hasPermission = false;
        }
      }
      if (hasPermission) {
        replies.add(new CacheDirectiveEntry(info, cur.getValue().toStats()));
        numReplies++;
      }
    }
    return new BatchedListEntries<CacheDirectiveEntry>(replies, false);
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
    CachePool pool;
    try {
      CachePoolInfo.validate(info);
      String poolName = info.getPoolName();
      pool = cachePools.get(poolName);
      if (pool != null) {
        throw new InvalidRequestException("Cache pool " + poolName
            + " already exists.");
      }
      pool = CachePool.createFromInfoAndDefaults(info);
      cachePools.put(pool.getPoolName(), pool);
    } catch (IOException e) {
      LOG.info("addCachePool of " + info + " failed: ", e);
      throw e;
    }
    LOG.info("addCachePool of " + info + " successful.");
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
    StringBuilder bld = new StringBuilder();
    try {
      CachePoolInfo.validate(info);
      String poolName = info.getPoolName();
      CachePool pool = cachePools.get(poolName);
      if (pool == null) {
        throw new InvalidRequestException("Cache pool " + poolName
            + " does not exist.");
      }
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
      if (info.getLimit() != null) {
        pool.setLimit(info.getLimit());
        bld.append(prefix).append("set limit to " + info.getLimit());
        prefix = "; ";
        // New limit changes stats, need to set needs refresh
        if (monitor != null) {
          monitor.setNeedsRescan();
        }
      }
      if (prefix.isEmpty()) {
        bld.append("no changes.");
      }
    } catch (IOException e) {
      LOG.info("modifyCachePool of " + info + " failed: ", e);
      throw e;
    }
    LOG.info("modifyCachePool of " + info.getPoolName() + " successful; "
        + bld.toString());
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
    try {
      CachePoolInfo.validateName(poolName);
      CachePool pool = cachePools.remove(poolName);
      if (pool == null) {
        throw new InvalidRequestException(
            "Cannot remove non-existent cache pool " + poolName);
      }
      // Remove all directives in this pool.
      Iterator<CacheDirective> iter = pool.getDirectiveList().iterator();
      while (iter.hasNext()) {
        CacheDirective directive = iter.next();
        directivesByPath.remove(directive.getPath());
        directivesById.remove(directive.getId());
        iter.remove();
      }
      if (monitor != null) {
        monitor.setNeedsRescan();
      }
    } catch (IOException e) {
      LOG.info("removeCachePool of " + poolName + " failed: ", e);
      throw e;
    }
    LOG.info("removeCachePool of " + poolName + " successful.");
  }

  public BatchedListEntries<CachePoolEntry>
      listCachePools(FSPermissionChecker pc, String prevKey) {
    assert namesystem.hasReadLock();
    if (monitor != null) {
      monitor.waitForRescanIfNeeded();
    }
    final int NUM_PRE_ALLOCATED_ENTRIES = 16;
    ArrayList<CachePoolEntry> results = 
        new ArrayList<CachePoolEntry>(NUM_PRE_ALLOCATED_ENTRIES);
    SortedMap<String, CachePool> tailMap = cachePools.tailMap(prevKey, false);
    int numListed = 0;
    for (Entry<String, CachePool> cur : tailMap.entrySet()) {
      if (numListed++ >= maxListCachePoolsResponses) {
        return new BatchedListEntries<CachePoolEntry>(results, true);
      }
      results.add(cur.getValue().getEntry(pc));
    }
    return new BatchedListEntries<CachePoolEntry>(results, false);
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
  public void saveState(DataOutputStream out, String sdPath)
      throws IOException {
    out.writeLong(nextDirectiveId);
    savePools(out, sdPath);
    saveDirectives(out, sdPath);
  }

  /**
   * Reloads CacheManager state from the passed DataInput. Used during namenode
   * startup to restore CacheManager state from an FSImage.
   * @param in DataInput from which to restore state
   * @throws IOException
   */
  public void loadState(DataInput in) throws IOException {
    nextDirectiveId = in.readLong();
    // pools need to be loaded first since directives point to their parent pool
    loadPools(in);
    loadDirectives(in);
  }

  /**
   * Save cache pools to fsimage
   */
  private void savePools(DataOutputStream out,
      String sdPath) throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.CACHE_POOLS, sdPath);
    prog.beginStep(Phase.SAVING_CHECKPOINT, step);
    prog.setTotal(Phase.SAVING_CHECKPOINT, step, cachePools.size());
    Counter counter = prog.getCounter(Phase.SAVING_CHECKPOINT, step);
    out.writeInt(cachePools.size());
    for (CachePool pool: cachePools.values()) {
      FSImageSerialization.writeCachePoolInfo(out, pool.getInfo(true));
      counter.increment();
    }
    prog.endStep(Phase.SAVING_CHECKPOINT, step);
  }

  /*
   * Save cache entries to fsimage
   */
  private void saveDirectives(DataOutputStream out, String sdPath)
      throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.CACHE_ENTRIES, sdPath);
    prog.beginStep(Phase.SAVING_CHECKPOINT, step);
    prog.setTotal(Phase.SAVING_CHECKPOINT, step, directivesById.size());
    Counter counter = prog.getCounter(Phase.SAVING_CHECKPOINT, step);
    out.writeInt(directivesById.size());
    for (CacheDirective directive : directivesById.values()) {
      FSImageSerialization.writeCacheDirectiveInfo(out, directive.toInfo());
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
      addCachePool(FSImageSerialization.readCachePoolInfo(in));
      counter.increment();
    }
    prog.endStep(Phase.LOADING_FSIMAGE, step);
  }

  /**
   * Load cache directives from the fsimage
   */
  private void loadDirectives(DataInput in) throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.CACHE_ENTRIES);
    prog.beginStep(Phase.LOADING_FSIMAGE, step);
    int numDirectives = in.readInt();
    prog.setTotal(Phase.LOADING_FSIMAGE, step, numDirectives);
    Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, step);
    for (int i = 0; i < numDirectives; i++) {
      CacheDirectiveInfo info = FSImageSerialization.readCacheDirectiveInfo(in);
      // Get pool reference by looking it up in the map
      final String poolName = info.getPool();
      CachePool pool = cachePools.get(poolName);
      if (pool == null) {
        throw new IOException("Directive refers to pool " + poolName +
            ", which does not exist.");
      }
      CacheDirective directive =
          new CacheDirective(info.getId(), info.getPath().toUri().getPath(),
              info.getReplication(), info.getExpiration().getAbsoluteMillis());
      boolean addedDirective = pool.getDirectiveList().add(directive);
      assert addedDirective;
      if (directivesById.put(directive.getId(), directive) != null) {
        throw new IOException("A directive with ID " + directive.getId() +
            " already exists");
      }
      List<CacheDirective> directives =
          directivesByPath.get(directive.getPath());
      if (directives == null) {
        directives = new LinkedList<CacheDirective>();
        directivesByPath.put(directive.getPath(), directives);
      }
      directives.add(directive);
      counter.increment();
    }
    prog.endStep(Phase.LOADING_FSIMAGE, step);
  }
}
