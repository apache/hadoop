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
package org.apache.hadoop.hdfs.server.federation.resolver;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_DEFAULT_NAMESERVICE;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreCache;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Mount table to map between global paths and remote locations. This allows the
 * {@link org.apache.hadoop.hdfs.server.federation.router.Router Router} to map
 * the global HDFS view to the remote namespaces. This is similar to
 * {@link org.apache.hadoop.fs.viewfs.ViewFs ViewFs}.
 * This is implemented as a tree.
 */
public class MountTableResolver
    implements FileSubclusterResolver, StateStoreCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(MountTableResolver.class);

  /** Reference to Router. */
  private final Router router;
  /** Reference to the State Store. */
  private final StateStoreService stateStore;
  /** Interface to the mount table store. */
  private MountTableStore mountTableStore;

  /** If the tree has been initialized. */
  private boolean init = false;
  /** Path -> Remote HDFS location. */
  private final TreeMap<String, MountTable> tree = new TreeMap<>();
  /** Path -> Remote location. */
  private final ConcurrentNavigableMap<String, PathLocation> locationCache =
      new ConcurrentSkipListMap<>();

  /** Default nameservice when no mount matches the math. */
  private String defaultNameService = "";

  /** Synchronization for both the tree and the cache. */
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();


  @VisibleForTesting
  public MountTableResolver(Configuration conf) {
    this(conf, (StateStoreService)null);
  }

  public MountTableResolver(Configuration conf, Router routerService) {
    this.router = routerService;
    if (this.router != null) {
      this.stateStore = this.router.getStateStore();
    } else {
      this.stateStore = null;
    }

    registerCacheExternal();
    initDefaultNameService(conf);
  }

  public MountTableResolver(Configuration conf, StateStoreService store) {
    this.router = null;
    this.stateStore = store;

    registerCacheExternal();
    initDefaultNameService(conf);
  }

  /**
   * Request cache updates from the State Store for this resolver.
   */
  private void registerCacheExternal() {
    if (this.stateStore != null) {
      this.stateStore.registerCacheExternal(this);
    }
  }

  /**
   * Nameservice for APIs that cannot be resolved to a specific one.
   *
   * @param conf Configuration for this resolver.
   */
  private void initDefaultNameService(Configuration conf) {
    try {
      this.defaultNameService = conf.get(
          DFS_ROUTER_DEFAULT_NAMESERVICE,
          DFSUtil.getNamenodeNameServiceId(conf));
    } catch (HadoopIllegalArgumentException e) {
      LOG.error("Cannot find default name service, setting it to the first");
      Collection<String> nsIds = DFSUtilClient.getNameServiceIds(conf);
      this.defaultNameService = nsIds.iterator().next();
      LOG.info("Default name service: {}", this.defaultNameService);
    }
  }

  /**
   * Get a reference for the Router for this resolver.
   *
   * @return Router for this resolver.
   */
  protected Router getRouter() {
    return this.router;
  }

  /**
   * Get the mount table store for this resolver.
   *
   * @return Mount table store.
   * @throws IOException If it cannot connect to the State Store.
   */
  protected MountTableStore getMountTableStore() throws IOException {
    if (this.mountTableStore == null) {
      this.mountTableStore = this.stateStore.getRegisteredRecordStore(
          MountTableStore.class);
      if (this.mountTableStore == null) {
        throw new IOException("State Store does not have an interface for " +
            MountTableStore.class);
      }
    }
    return this.mountTableStore;
  }

  /**
   * Add a mount entry to the table.
   *
   * @param entry The mount table record to add from the state store.
   */
  public void addEntry(final MountTable entry) {
    writeLock.lock();
    try {
      String srcPath = entry.getSourcePath();
      this.tree.put(srcPath, entry);
      invalidateLocationCache(srcPath);
    } finally {
      writeLock.unlock();
    }
    this.init = true;
  }

  /**
   * Remove a mount table entry.
   *
   * @param srcPath Source path for the entry to remove.
   */
  public void removeEntry(final String srcPath) {
    writeLock.lock();
    try {
      this.tree.remove(srcPath);
      invalidateLocationCache(srcPath);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Invalidates all cache entries below this path. It requires the write lock.
   *
   * @param path Source path.
   */
  private void invalidateLocationCache(final String path) {
    if (locationCache.isEmpty()) {
      return;
    }
    // Determine next lexicographic entry after source path
    String nextSrc = path + Character.MAX_VALUE;
    ConcurrentNavigableMap<String, PathLocation> subMap =
        locationCache.subMap(path, nextSrc);
    for (final String key : subMap.keySet()) {
      locationCache.remove(key);
    }
  }

  /**
   * Updates the mount path tree with a new set of mount table entries. It also
   * updates the needed caches.
   *
   * @param entries Full set of mount table entries to update.
   */
  @VisibleForTesting
  public void refreshEntries(final Collection<MountTable> entries) {
    // The tree read/write must be atomic
    writeLock.lock();
    try {
      // New entries
      Map<String, MountTable> newEntries = new ConcurrentHashMap<>();
      for (MountTable entry : entries) {
        String srcPath = entry.getSourcePath();
        newEntries.put(srcPath, entry);
      }

      // Old entries (reversed to sort from the leaves to the root)
      Set<String> oldEntries = new TreeSet<>(Collections.reverseOrder());
      for (MountTable entry : getTreeValues("/")) {
        String srcPath = entry.getSourcePath();
        oldEntries.add(srcPath);
      }

      // Entries that need to be removed
      for (String srcPath : oldEntries) {
        if (!newEntries.containsKey(srcPath)) {
          this.tree.remove(srcPath);
          invalidateLocationCache(srcPath);
          LOG.info("Removed stale mount point {} from resolver", srcPath);
        }
      }

      // Entries that need to be added
      for (MountTable entry : entries) {
        String srcPath = entry.getSourcePath();
        if (!oldEntries.contains(srcPath)) {
          // Add node, it does not exist
          this.tree.put(srcPath, entry);
          LOG.info("Added new mount point {} to resolver", srcPath);
        } else {
          // Node exists, check for updates
          MountTable existingEntry = this.tree.get(srcPath);
          if (existingEntry != null && !existingEntry.equals(entry)) {
            // Entry has changed
            invalidateLocationCache(srcPath);
            LOG.info("Updated mount point {} in resolver");
          }
        }
      }
    } finally {
      writeLock.unlock();
    }
    this.init = true;
  }

  /**
   * Replaces the current in-memory cached of the mount table with a new
   * version fetched from the data store.
   */
  @Override
  public boolean loadCache(boolean force) {
    try {
      // Our cache depends on the store, update it first
      MountTableStore mountTable = this.getMountTableStore();
      mountTable.loadCache(force);

      GetMountTableEntriesRequest request =
          GetMountTableEntriesRequest.newInstance("/");
      GetMountTableEntriesResponse response =
          mountTable.getMountTableEntries(request);
      List<MountTable> records = response.getEntries();
      refreshEntries(records);
    } catch (IOException e) {
      LOG.error("Cannot fetch mount table entries from State Store", e);
      return false;
    }
    return true;
  }

  /**
   * Clears all data.
   */
  public void clear() {
    LOG.info("Clearing all mount location caches");
    writeLock.lock();
    try {
      this.locationCache.clear();
      this.tree.clear();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public PathLocation getDestinationForPath(final String path)
      throws IOException {
    verifyMountTable();
    readLock.lock();
    try {
      return this.locationCache.computeIfAbsent(
          path, this::lookupLocation);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Build the path location to insert into the cache atomically. It must hold
   * the read lock.
   * @param path Path to check/insert.
   * @return New remote location.
   */
  public PathLocation lookupLocation(final String path) {
    PathLocation ret = null;
    MountTable entry = findDeepest(path);
    if (entry != null) {
      ret = buildLocation(path, entry);
    } else {
      // Not found, use default location
      RemoteLocation remoteLocation =
          new RemoteLocation(defaultNameService, path);
      List<RemoteLocation> locations =
          Collections.singletonList(remoteLocation);
      ret = new PathLocation(null, locations);
    }
    return ret;
  }

  /**
   * Get the mount table entry for a path.
   *
   * @param path Path to look for.
   * @return Mount table entry the path belongs.
   * @throws IOException If the State Store could not be reached.
   */
  public MountTable getMountPoint(final String path) throws IOException {
    verifyMountTable();
    return findDeepest(path);
  }

  @Override
  public List<String> getMountPoints(final String path) throws IOException {
    verifyMountTable();

    Set<String> children = new TreeSet<>();
    readLock.lock();
    try {
      String from = path;
      String to = path + Character.MAX_VALUE;
      SortedMap<String, MountTable> subMap = this.tree.subMap(from, to);

      boolean exists = false;
      for (String subPath : subMap.keySet()) {
        String child = subPath;

        // Special case for /
        if (!path.equals(Path.SEPARATOR)) {
          // Get the children
          int ini = path.length();
          child = subPath.substring(ini);
        }

        if (child.isEmpty()) {
          // This is a mount point but without children
          exists = true;
        } else if (child.startsWith(Path.SEPARATOR)) {
          // This is a mount point with children
          exists = true;
          child = child.substring(1);

          // We only return immediate children
          int fin = child.indexOf(Path.SEPARATOR);
          if (fin > -1) {
            child = child.substring(0, fin);
          }
          if (!child.isEmpty()) {
            children.add(child);
          }
        }
      }
      if (!exists) {
        return null;
      }
      return new LinkedList<>(children);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get all the mount records at or beneath a given path.
   * @param path Path to get the mount points from.
   * @return List of mount table records under the path or null if the path is
   *         not found.
   * @throws IOException If it's not connected to the State Store.
   */
  public List<MountTable> getMounts(final String path) throws IOException {
    verifyMountTable();

    return getTreeValues(path, false);
  }

  /**
   * Check if the Mount Table is ready to be used.
   * @throws StateStoreUnavailableException If it cannot connect to the store.
   */
  private void verifyMountTable() throws StateStoreUnavailableException {
    if (!this.init) {
      throw new StateStoreUnavailableException("Mount Table not initialized");
    }
  }

  @Override
  public String toString() {
    readLock.lock();
    try {
      return this.tree.toString();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Build a location for this result beneath the discovered mount point.
   *
   * @param path Path to build for.
   * @param entry Mount table entry.
   * @return PathLocation containing the namespace, local path.
   */
  private static PathLocation buildLocation(
      final String path, final MountTable entry) {

    String srcPath = entry.getSourcePath();
    if (!path.startsWith(srcPath)) {
      LOG.error("Cannot build location, {} not a child of {}", path, srcPath);
      return null;
    }
    String remainingPath = path.substring(srcPath.length());
    if (remainingPath.startsWith(Path.SEPARATOR)) {
      remainingPath = remainingPath.substring(1);
    }

    List<RemoteLocation> locations = new LinkedList<>();
    for (RemoteLocation oneDst : entry.getDestinations()) {
      String nsId = oneDst.getNameserviceId();
      String dest = oneDst.getDest();
      String newPath = dest;
      if (!newPath.endsWith(Path.SEPARATOR)) {
        newPath += Path.SEPARATOR;
      }
      newPath += remainingPath;
      RemoteLocation remoteLocation = new RemoteLocation(nsId, newPath);
      locations.add(remoteLocation);
    }
    DestinationOrder order = entry.getDestOrder();
    return new PathLocation(srcPath, locations, order);
  }

  @Override
  public String getDefaultNamespace() {
    return this.defaultNameService;
  }

  /**
   * Find the deepest mount point for a path.
   * @param path Path to look for.
   * @return Mount table entry.
   */
  private MountTable findDeepest(final String path) {
    readLock.lock();
    try {
      Entry<String, MountTable> entry = this.tree.floorEntry(path);
      while (entry != null && !path.startsWith(entry.getKey())) {
        entry = this.tree.lowerEntry(entry.getKey());
      }
      if (entry == null) {
        return null;
      }
      return entry.getValue();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get the mount table entries under a path.
   * @param path Path to search from.
   * @return Mount Table entries.
   */
  private List<MountTable> getTreeValues(final String path) {
    return getTreeValues(path, false);
  }

  /**
   * Get the mount table entries under a path.
   * @param path Path to search from.
   * @param reverse If the order should be reversed.
   * @return Mount Table entries.
   */
  private List<MountTable> getTreeValues(final String path, boolean reverse) {
    LinkedList<MountTable> ret = new LinkedList<>();
    readLock.lock();
    try {
      String from = path;
      String to = path + Character.MAX_VALUE;
      SortedMap<String, MountTable> subMap = this.tree.subMap(from, to);
      for (MountTable entry : subMap.values()) {
        if (!reverse) {
          ret.add(entry);
        } else {
          ret.addFirst(entry);
        }
      }
    } finally {
      readLock.unlock();
    }
    return ret;
  }
}
