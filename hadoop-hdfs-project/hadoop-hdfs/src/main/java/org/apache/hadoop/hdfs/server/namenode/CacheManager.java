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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT_DEFAULT;
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
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo.Expiration;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveStats;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.CacheReplicationMonitor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList.Type;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

/**
 * The Cache Manager handles caching on DataNodes.
 *
 * This class is instantiated by the FSNamesystem.
 * It maintains the mapping of cached blocks to datanodes via processing
 * datanode cache reports. Based on these reports and addition and removal of
 * caching directives, we will schedule caching and uncaching work.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public final class CacheManager {
  public static final Logger LOG = LoggerFactory.getLogger(CacheManager.class);

  private static final float MIN_CACHED_BLOCKS_PERCENT = 0.001f;

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
   * All cached blocks.
   */
  private final GSet<CachedBlock, CachedBlock> cachedBlocks;

  /**
   * Lock which protects the CacheReplicationMonitor.
   */
  private final ReentrantLock crmLock = new ReentrantLock();

  private final SerializerCompat serializerCompat = new SerializerCompat();

  /**
   * The CacheReplicationMonitor.
   */
  private CacheReplicationMonitor monitor;

  public static final class PersistState {
    public final CacheManagerSection section;
    public final List<CachePoolInfoProto> pools;
    public final List<CacheDirectiveInfoProto> directives;

    public PersistState(CacheManagerSection section,
        List<CachePoolInfoProto> pools, List<CacheDirectiveInfoProto> directives) {
      this.section = section;
      this.pools = pools;
      this.directives = directives;
    }
  }

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
    float cachedBlocksPercent = conf.getFloat(
          DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT,
          DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT_DEFAULT);
    if (cachedBlocksPercent < MIN_CACHED_BLOCKS_PERCENT) {
      LOG.info("Using minimum value {} for {}", MIN_CACHED_BLOCKS_PERCENT,
        DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT);
      cachedBlocksPercent = MIN_CACHED_BLOCKS_PERCENT;
    }
    this.cachedBlocks = new LightWeightGSet<CachedBlock, CachedBlock>(
          LightWeightGSet.computeCapacity(cachedBlocksPercent,
              "cachedBlocks"));

  }

  /**
   * Resets all tracked directives and pools. Called during 2NN checkpointing to
   * reset FSNamesystem state. See {@link FSNamesystem#clear()}.
   */
  void clear() {
    directivesById.clear();
    directivesByPath.clear();
    cachePools.clear();
    nextDirectiveId = 1;
  }

  public void startMonitorThread() {
    crmLock.lock();
    try {
      if (this.monitor == null) {
        this.monitor = new CacheReplicationMonitor(namesystem, this,
            scanIntervalMs, crmLock);
        this.monitor.start();
      }
    } finally {
      crmLock.unlock();
    }
  }

  public void stopMonitorThread() {
    crmLock.lock();
    try {
      if (this.monitor != null) {
        CacheReplicationMonitor prevMonitor = this.monitor;
        this.monitor = null;
        IOUtils.closeQuietly(prevMonitor);
      }
    } finally {
      crmLock.unlock();
    }
  }

  public void clearDirectiveStats() {
    assert namesystem.hasWriteLock();
    for (CacheDirective directive : directivesById.values()) {
      directive.resetStatistics();
    }
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
   * @param info to validate.
   * @param maxRelativeExpiryTime of the info's pool.
   * @return the expiration time, or the pool's max absolute expiration if the
   *         info's expiration was not set.
   * @throws InvalidRequestException if the info's Expiration is invalid.
   */
  private static long validateExpiryTime(CacheDirectiveInfo info,
      long maxRelativeExpiryTime) throws InvalidRequestException {
    LOG.trace("Validating directive {} pool maxRelativeExpiryTime {}", info,
        maxRelativeExpiryTime);
    final long now = new Date().getTime();
    final long maxAbsoluteExpiryTime = now + maxRelativeExpiryTime;
    if (info == null || info.getExpiration() == null) {
      return maxAbsoluteExpiryTime;
    }
    Expiration expiry = info.getExpiration();
    if (expiry.getMillis() < 0l) {
      throw new InvalidRequestException("Cannot set a negative expiration: "
          + expiry.getMillis());
    }
    long relExpiryTime, absExpiryTime;
    if (expiry.isRelative()) {
      relExpiryTime = expiry.getMillis();
      absExpiryTime = now + relExpiryTime;
    } else {
      absExpiryTime = expiry.getMillis();
      relExpiryTime = absExpiryTime - now;
    }
    // Need to cap the expiry so we don't overflow a long when doing math
    if (relExpiryTime > Expiration.MAX_RELATIVE_EXPIRY_MS) {
      throw new InvalidRequestException("Expiration "
          + expiry.toString() + " is too far in the future!");
    }
    // Fail if the requested expiry is greater than the max
    if (relExpiryTime > maxRelativeExpiryTime) {
      throw new InvalidRequestException("Expiration " + expiry.toString()
          + " exceeds the max relative expiration time of "
          + maxRelativeExpiryTime + " ms.");
    }
    return absExpiryTime;
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
    if (pool.getLimit() == CachePoolInfo.LIMIT_UNLIMITED) {
      return;
    }
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
      ReadOnlyList<INode> children = dir
          .getChildrenList(Snapshot.CURRENT_STATE_ID);
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

    setNeedsRescan();
  }

  /**
   * Adds a directive, skipping most error checking. This should only be called
   * internally in special scenarios like edit log replay.
   */
  CacheDirectiveInfo addDirectiveFromEditLog(CacheDirectiveInfo directive)
      throws InvalidRequestException {
    long id = directive.getId();
    CacheDirective entry = new CacheDirective(directive);
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
      long expiryTime = validateExpiryTime(info, pool.getMaxRelativeExpiryMs());
      // Do quota validation if required
      if (!flags.contains(CacheFlag.FORCE)) {
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
    LOG.info("addDirective of {} successful.", info);
    return directive.toInfo();
  }

  /**
   * Factory method that makes a new CacheDirectiveInfo by applying fields in a
   * CacheDirectiveInfo to an existing CacheDirective.
   * 
   * @param info with some or all fields set.
   * @param defaults directive providing default values for unset fields in
   *          info.
   * 
   * @return new CacheDirectiveInfo of the info applied to the defaults.
   */
  private static CacheDirectiveInfo createFromInfoAndDefaults(
      CacheDirectiveInfo info, CacheDirective defaults) {
    // Initialize the builder with the default values
    CacheDirectiveInfo.Builder builder =
        new CacheDirectiveInfo.Builder(defaults.toInfo());
    // Replace default with new value if present
    if (info.getPath() != null) {
      builder.setPath(info.getPath());
    }
    if (info.getReplication() != null) {
      builder.setReplication(info.getReplication());
    }
    if (info.getPool() != null) {
      builder.setPool(info.getPool());
    }
    if (info.getExpiration() != null) {
      builder.setExpiration(info.getExpiration());
    }
    return builder.build();
  }

  /**
   * Modifies a directive, skipping most error checking. This is for careful
   * internal use only. modifyDirective can be non-deterministic since its error
   * checking depends on current system time, which poses a problem for edit log
   * replay.
   */
  void modifyDirectiveFromEditLog(CacheDirectiveInfo info)
      throws InvalidRequestException {
    // Check for invalid IDs.
    Long id = info.getId();
    if (id == null) {
      throw new InvalidRequestException("Must supply an ID.");
    }
    CacheDirective prevEntry = getById(id);
    CacheDirectiveInfo newInfo = createFromInfoAndDefaults(info, prevEntry);
    removeInternal(prevEntry);
    addInternal(new CacheDirective(newInfo), getCachePool(newInfo.getPool()));
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

      // Fill in defaults
      CacheDirectiveInfo infoWithDefaults =
          createFromInfoAndDefaults(info, prevEntry);
      CacheDirectiveInfo.Builder builder =
          new CacheDirectiveInfo.Builder(infoWithDefaults);

      // Do validation
      validatePath(infoWithDefaults);
      validateReplication(infoWithDefaults, (short)-1);
      // Need to test the pool being set here to avoid rejecting a modify for a
      // directive that's already been forced into a pool
      CachePool srcPool = prevEntry.getPool();
      CachePool destPool = getCachePool(validatePoolName(infoWithDefaults));
      if (!srcPool.getPoolName().equals(destPool.getPoolName())) {
        checkWritePermission(pc, destPool);
        if (!flags.contains(CacheFlag.FORCE)) {
          checkLimit(destPool, infoWithDefaults.getPath().toUri().getPath(),
              infoWithDefaults.getReplication());
        }
      }
      // Verify the expiration against the destination pool
      validateExpiryTime(infoWithDefaults, destPool.getMaxRelativeExpiryMs());

      // Indicate changes to the CRM
      setNeedsRescan();

      // Validation passed
      removeInternal(prevEntry);
      addInternal(new CacheDirective(builder.build()), destPool);
    } catch (IOException e) {
      LOG.warn("modifyDirective of " + idString + " failed: ", e);
      throw e;
    }
    LOG.info("modifyDirective of {} successfully applied {}.", idString, info);
  }

  private void removeInternal(CacheDirective directive)
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

    setNeedsRescan();
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
    if (filter.getPath() != null) {
      filterPath = validatePath(filter);
    }
    if (filter.getReplication() != null) {
      throw new InvalidRequestException(
          "Filtering by replication is unsupported.");
    }

    // Querying for a single ID
    final Long id = filter.getId();
    if (id != null) {
      if (!directivesById.containsKey(id)) {
        throw new InvalidRequestException("Did not find requested id " + id);
      }
      // Since we use a tailMap on directivesById, setting prev to id-1 gets
      // us the directive with the id (if present)
      prevId = id - 1;
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

      // If the requested ID is present, it should be the first item.
      // Hitting this case means the ID is not present, or we're on the second
      // item and should break out.
      if (id != null &&
          !(info.getId().equals(id))) {
        break;
      }
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
    LOG.info("addCachePool of {} successful.", info);
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
        setNeedsRescan();
      }
      if (info.getMaxRelativeExpiryMs() != null) {
        final Long maxRelativeExpiry = info.getMaxRelativeExpiryMs();
        pool.setMaxRelativeExpiryMs(maxRelativeExpiry);
        bld.append(prefix).append("set maxRelativeExpiry to "
            + maxRelativeExpiry);
        prefix = "; ";
      }
      if (prefix.isEmpty()) {
        bld.append("no changes.");
      }
    } catch (IOException e) {
      LOG.info("modifyCachePool of " + info + " failed: ", e);
      throw e;
    }
    LOG.info("modifyCachePool of {} successful; {}", info.getPoolName(), 
        bld.toString());
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
      setNeedsRescan();
    } catch (IOException e) {
      LOG.info("removeCachePool of " + poolName + " failed: ", e);
      throw e;
    }
    LOG.info("removeCachePool of " + poolName + " successful.");
  }

  public BatchedListEntries<CachePoolEntry>
      listCachePools(FSPermissionChecker pc, String prevKey) {
    assert namesystem.hasReadLock();
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
    namesystem.writeLock();
    final long startTime = Time.monotonicNow();
    final long endTime;
    try {
      final DatanodeDescriptor datanode = 
          blockManager.getDatanodeManager().getDatanode(datanodeID);
      if (datanode == null || !datanode.isRegistered()) {
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
    LOG.debug("Processed cache report from {}, blocks: {}, " +
        "processing time: {} msecs", datanodeID, blockIds.size(), 
        (endTime - startTime));
  }

  private void processCacheReportImpl(final DatanodeDescriptor datanode,
      final List<Long> blockIds) {
    CachedBlocksList cached = datanode.getCached();
    cached.clear();
    CachedBlocksList cachedList = datanode.getCached();
    CachedBlocksList pendingCachedList = datanode.getPendingCached();
    for (Iterator<Long> iter = blockIds.iterator(); iter.hasNext(); ) {
      long blockId = iter.next();
      LOG.trace("Cache report from datanode {} has block {}", datanode,
          blockId);
      CachedBlock cachedBlock =
          new CachedBlock(blockId, (short)0, false);
      CachedBlock prevCachedBlock = cachedBlocks.get(cachedBlock);
      // Add the block ID from the cache report to the cachedBlocks map
      // if it's not already there.
      if (prevCachedBlock != null) {
        cachedBlock = prevCachedBlock;
      } else {
        cachedBlocks.put(cachedBlock);
        LOG.trace("Added block {}  to cachedBlocks", cachedBlock);
      }
      // Add the block to the datanode's implicit cached block list
      // if it's not already there.  Similarly, remove it from the pending
      // cached block list if it exists there.
      if (!cachedBlock.isPresent(cachedList)) {
        cachedList.add(cachedBlock);
        LOG.trace("Added block {} to CACHED list.", cachedBlock);
      }
      if (cachedBlock.isPresent(pendingCachedList)) {
        pendingCachedList.remove(cachedBlock);
        LOG.trace("Removed block {} from PENDING_CACHED list.", cachedBlock);
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
  public void saveStateCompat(DataOutputStream out, String sdPath)
      throws IOException {
    serializerCompat.save(out, sdPath);
  }

  public PersistState saveState() throws IOException {
    ArrayList<CachePoolInfoProto> pools = Lists
        .newArrayListWithCapacity(cachePools.size());
    ArrayList<CacheDirectiveInfoProto> directives = Lists
        .newArrayListWithCapacity(directivesById.size());

    for (CachePool pool : cachePools.values()) {
      CachePoolInfo p = pool.getInfo(true);
      CachePoolInfoProto.Builder b = CachePoolInfoProto.newBuilder()
          .setPoolName(p.getPoolName());

      if (p.getOwnerName() != null)
        b.setOwnerName(p.getOwnerName());

      if (p.getGroupName() != null)
        b.setGroupName(p.getGroupName());

      if (p.getMode() != null)
        b.setMode(p.getMode().toShort());

      if (p.getLimit() != null)
        b.setLimit(p.getLimit());

      pools.add(b.build());
    }

    for (CacheDirective directive : directivesById.values()) {
      CacheDirectiveInfo info = directive.toInfo();
      CacheDirectiveInfoProto.Builder b = CacheDirectiveInfoProto.newBuilder()
          .setId(info.getId());

      if (info.getPath() != null) {
        b.setPath(info.getPath().toUri().getPath());
      }

      if (info.getReplication() != null) {
        b.setReplication(info.getReplication());
      }

      if (info.getPool() != null) {
        b.setPool(info.getPool());
      }

      Expiration expiry = info.getExpiration();
      if (expiry != null) {
        assert (!expiry.isRelative());
        b.setExpiration(PBHelper.convert(expiry));
      }

      directives.add(b.build());
    }
    CacheManagerSection s = CacheManagerSection.newBuilder()
        .setNextDirectiveId(nextDirectiveId).setNumPools(pools.size())
        .setNumDirectives(directives.size()).build();

    return new PersistState(s, pools, directives);
  }

  /**
   * Reloads CacheManager state from the passed DataInput. Used during namenode
   * startup to restore CacheManager state from an FSImage.
   * @param in DataInput from which to restore state
   * @throws IOException
   */
  public void loadStateCompat(DataInput in) throws IOException {
    serializerCompat.load(in);
  }

  public void loadState(PersistState s) throws IOException {
    nextDirectiveId = s.section.getNextDirectiveId();
    for (CachePoolInfoProto p : s.pools) {
      CachePoolInfo info = new CachePoolInfo(p.getPoolName());
      if (p.hasOwnerName())
        info.setOwnerName(p.getOwnerName());

      if (p.hasGroupName())
        info.setGroupName(p.getGroupName());

      if (p.hasMode())
        info.setMode(new FsPermission((short) p.getMode()));

      if (p.hasLimit())
        info.setLimit(p.getLimit());

      addCachePool(info);
    }

    for (CacheDirectiveInfoProto p : s.directives) {
      // Get pool reference by looking it up in the map
      final String poolName = p.getPool();
      CacheDirective directive = new CacheDirective(p.getId(), new Path(
          p.getPath()).toUri().getPath(), (short) p.getReplication(), p
          .getExpiration().getMillis());
      addCacheDirective(poolName, directive);
    }
  }

  private void addCacheDirective(final String poolName,
      final CacheDirective directive) throws IOException {
    CachePool pool = cachePools.get(poolName);
    if (pool == null) {
      throw new IOException("Directive refers to pool " + poolName
          + ", which does not exist.");
    }
    boolean addedDirective = pool.getDirectiveList().add(directive);
    assert addedDirective;
    if (directivesById.put(directive.getId(), directive) != null) {
      throw new IOException("A directive with ID " + directive.getId()
          + " already exists");
    }
    List<CacheDirective> directives = directivesByPath.get(directive.getPath());
    if (directives == null) {
      directives = new LinkedList<CacheDirective>();
      directivesByPath.put(directive.getPath(), directives);
    }
    directives.add(directive);
  }

  private final class SerializerCompat {
    private void save(DataOutputStream out, String sdPath) throws IOException {
      out.writeLong(nextDirectiveId);
      savePools(out, sdPath);
      saveDirectives(out, sdPath);
    }

    private void load(DataInput in) throws IOException {
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
        CacheDirective directive =
            new CacheDirective(info.getId(), info.getPath().toUri().getPath(),
                info.getReplication(), info.getExpiration().getAbsoluteMillis());
        addCacheDirective(poolName, directive);
        counter.increment();
      }
      prog.endStep(Phase.LOADING_FSIMAGE, step);
    }
  }

  public void waitForRescanIfNeeded() {
    crmLock.lock();
    try {
      if (monitor != null) {
        monitor.waitForRescanIfNeeded();
      }
    } finally {
      crmLock.unlock();
    }
  }

  private void setNeedsRescan() {
    crmLock.lock();
    try {
      if (monitor != null) {
        monitor.setNeedsRescan();
      }
    } finally {
      crmLock.unlock();
    }
  }

  @VisibleForTesting
  public Thread getCacheReplicationMonitor() {
    crmLock.lock();
    try {
      return monitor;
    } finally {
      crmLock.unlock();
    }
  }
}
