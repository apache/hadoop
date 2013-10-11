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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DESCRIPTORS_NUM_RESPONSES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DESCRIPTORS_NUM_RESPONSES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT;

import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.InvalidPoolNameError;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.PoolWritePermissionDeniedError;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDescriptor;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDirective;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheEntry;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheDescriptorException.InvalidIdException;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheDescriptorException.NoSuchIdException;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheDescriptorException.RemovePermissionDeniedException;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheDescriptorException.UnexpectedRemovePathBasedCacheDescriptorException;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

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

  /**
   * Returns the next entry ID to be used for a PathBasedCacheEntry
   */
  synchronized long getNextEntryId() {
    Preconditions.checkArgument(nextEntryId != Long.MAX_VALUE);
    return nextEntryId++;
  }

  /**
   * Returns the PathBasedCacheEntry corresponding to a PathBasedCacheEntry.
   * 
   * @param directive Lookup directive
   * @return Corresponding PathBasedCacheEntry, or null if not present.
   */
  private synchronized PathBasedCacheEntry
      findEntry(PathBasedCacheDirective directive) {
    List<PathBasedCacheEntry> existing =
        entriesByPath.get(directive.getPath().toUri().getPath());
    if (existing == null) {
      return null;
    }
    for (PathBasedCacheEntry entry : existing) {
      if (entry.getPool().getPoolName().equals(directive.getPool())) {
        return entry;
      }
    }
    return null;
  }

  /**
   * Add a new PathBasedCacheEntry, skipping any validation checks. Called
   * directly when reloading CacheManager state from FSImage.
   * 
   * @throws IOException if unable to cache the entry
   */
  private void unprotectedAddEntry(PathBasedCacheEntry entry)
      throws IOException {
    assert namesystem.hasWriteLock();
    // Add it to the various maps
    entriesById.put(entry.getEntryId(), entry);
    String path = entry.getPath();
    List<PathBasedCacheEntry> entryList = entriesByPath.get(path);
    if (entryList == null) {
      entryList = new ArrayList<PathBasedCacheEntry>(1);
      entriesByPath.put(path, entryList);
    }
    entryList.add(entry);
    // Set the path as cached in the namesystem
    try {
      INode node = dir.getINode(entry.getPath());
      if (node != null && node.isFile()) {
        INodeFile file = node.asFile();
        // TODO: adjustable cache replication factor
        namesystem.setCacheReplicationInt(entry.getPath(),
            file.getBlockReplication());
      } else {
        LOG.warn("Path " + entry.getPath() + " is not a file");
      }
    } catch (IOException ioe) {
      LOG.info("unprotectedAddEntry " + entry +": failed to cache file: " +
          ioe.getClass().getName() +": " + ioe.getMessage());
      throw ioe;
    }
  }

  /**
   * Add a new PathBasedCacheDirective if valid, returning a corresponding
   * PathBasedCacheDescriptor to the user.
   * 
   * @param directive Directive describing the cache entry being added
   * @param pc Permission checker used to validate that the calling user has
   *          access to the destination cache pool
   * @return Corresponding PathBasedCacheDescriptor for the new cache entry
   * @throws IOException if the directive is invalid or was otherwise
   *           unsuccessful
   */
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

    // Success!
    PathBasedCacheDescriptor d = unprotectedAddDirective(directive);
    LOG.info("addDirective " + directive + ": added cache directive "
        + directive);
    return d;
  }

  /**
   * Assigns a new entry ID to a validated PathBasedCacheDirective and adds
   * it to the CacheManager. Called directly when replaying the edit log.
   * 
   * @param directive Directive being added
   * @return PathBasedCacheDescriptor for the directive
   * @throws IOException
   */
  PathBasedCacheDescriptor unprotectedAddDirective(
      PathBasedCacheDirective directive) throws IOException {
    assert namesystem.hasWriteLock();
    CachePool pool = cachePools.get(directive.getPool());
    // Add a new entry with the next available ID.
    PathBasedCacheEntry entry;
    entry = new PathBasedCacheEntry(getNextEntryId(),
        directive.getPath().toUri().getPath(), pool);

    unprotectedAddEntry(entry);

    return entry.getDescriptor();
  }

  /**
   * Remove the PathBasedCacheEntry corresponding to a descriptor ID from
   * the CacheManager.
   * 
   * @param id of the PathBasedCacheDescriptor
   * @param pc Permissions checker used to validated the request
   * @throws IOException
   */
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
    
    unprotectedRemoveDescriptor(id);
  }

  /**
   * Unchecked internal method used to remove a PathBasedCacheEntry from the
   * CacheManager. Called directly when replaying the edit log.
   * 
   * @param id of the PathBasedCacheDescriptor corresponding to the entry that
   *          is being removed
   * @throws IOException
   */
  void unprotectedRemoveDescriptor(long id) throws IOException {
    assert namesystem.hasWriteLock();
    PathBasedCacheEntry existing = entriesById.get(id);
    // Remove the corresponding entry in entriesByPath.
    String path = existing.getDescriptor().getPath().toUri().getPath();
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
      INode node = dir.getINode(existing.getDescriptor().getPath().toUri().
          getPath());
      if (node != null && node.isFile()) {
        namesystem.setCacheReplicationInt(existing.getDescriptor().getPath().
            toUri().getPath(), (short) 0);
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
   * @param info The info for the cache pool to create.
   * @return the created CachePool
   */
  public synchronized CachePoolInfo addCachePool(CachePoolInfo info)
      throws IOException {
    CachePoolInfo.validate(info);
    String poolName = info.getPoolName();
    CachePool pool = cachePools.get(poolName);
    if (pool != null) {
      throw new IOException("cache pool " + poolName + " already exists.");
    }
    pool = CachePool.createFromInfoAndDefaults(info);
    cachePools.put(pool.getPoolName(), pool);
    return pool.getInfo(true);
  }

  /**
   * Internal unchecked method used to add a CachePool. Called directly when
   * reloading CacheManager state from the FSImage or edit log.
   * 
   * @param pool to be added
   */
  void unprotectedAddCachePool(CachePoolInfo info) {
    assert namesystem.hasWriteLock();
    CachePool pool = CachePool.createFromInfo(info);
    cachePools.put(pool.getPoolName(), pool);
    LOG.info("created new cache pool " + pool);
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
        entriesByPath.remove(entry.getValue().getPath());
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

  /*
   * FSImage related serialization and deserialization code
   */

  /**
   * Saves the current state of the CacheManager to the DataOutput. Used
   * to persist CacheManager state in the FSImage.
   * @param out DataOutput to persist state
   * @param sdPath path of the storage directory
   * @throws IOException
   */
  public synchronized void saveState(DataOutput out, String sdPath)
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
  public synchronized void loadState(DataInput in) throws IOException {
    nextEntryId = in.readLong();
    // pools need to be loaded first since entries point to their parent pool
    loadPools(in);
    loadEntries(in);
  }

  /**
   * Save cache pools to fsimage
   */
  private synchronized void savePools(DataOutput out,
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
  private synchronized void saveEntries(DataOutput out, String sdPath)
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
      Text.writeString(out, entry.getPool().getPoolName());
      counter.increment();
    }
    prog.endStep(Phase.SAVING_CHECKPOINT, step);
  }

  /**
   * Load cache pools from fsimage
   */
  private synchronized void loadPools(DataInput in)
      throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.CACHE_POOLS);
    prog.beginStep(Phase.LOADING_FSIMAGE, step);
    int numberOfPools = in.readInt();
    prog.setTotal(Phase.LOADING_FSIMAGE, step, numberOfPools);
    Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, step);
    for (int i = 0; i < numberOfPools; i++) {
      CachePoolInfo info = CachePoolInfo.readFrom(in);
      unprotectedAddCachePool(info);
      counter.increment();
    }
    prog.endStep(Phase.LOADING_FSIMAGE, step);
  }

  /**
   * Load cache entries from the fsimage
   */
  private synchronized void loadEntries(DataInput in) throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.CACHE_ENTRIES);
    prog.beginStep(Phase.LOADING_FSIMAGE, step);
    int numberOfEntries = in.readInt();
    prog.setTotal(Phase.LOADING_FSIMAGE, step, numberOfEntries);
    Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, step);
    for (int i = 0; i < numberOfEntries; i++) {
      long entryId = in.readLong();
      String path = Text.readString(in);
      String poolName = Text.readString(in);
      // Get pool reference by looking it up in the map
      CachePool pool = cachePools.get(poolName);
      PathBasedCacheEntry entry = new PathBasedCacheEntry(entryId, path, pool);
      unprotectedAddEntry(entry);
      counter.increment();
    }
    prog.endStep(Phase.LOADING_FSIMAGE, step);
  }

}
