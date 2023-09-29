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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.hdfs.DFSUtil.isParentEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Module that implements the quota relevant RPC calls
 * {@link ClientProtocol#setQuota(String, long, long, StorageType)}
 * and
 * {@link ClientProtocol#getQuotaUsage(String)}
 * in the {@link RouterRpcServer}.
 */
public class Quota {
  private static final Logger LOG = LoggerFactory.getLogger(Quota.class);

  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;
  /** Router used in RouterRpcServer. */
  private final Router router;

  public Quota(Router router, RouterRpcServer server) {
    this.router = router;
    this.rpcServer = server;
    this.rpcClient = server.getRPCClient();
  }

  /**
   * Set quota for the federation path.
   * @param path Federation path.
   * @param namespaceQuota Name space quota.
   * @param storagespaceQuota Storage space quota.
   * @param type StorageType that the space quota is intended to be set on.
   * @param checkMountEntry whether to check the path is a mount entry.
   * @throws AccessControlException If the quota system is disabled or if
   * checkMountEntry is true and the path is a mount entry.
   */
  public void setQuota(String path, long namespaceQuota, long storagespaceQuota,
      StorageType type, boolean checkMountEntry) throws IOException {
    if (!router.isQuotaEnabled()) {
      throw new IOException("The quota system is disabled in Router.");
    }
    if (checkMountEntry && isMountEntry(path)) {
      throw new AccessControlException(
          "Permission denied: " + RouterRpcServer.getRemoteUser()
              + " is not allowed to change quota of " + path);
    }
    setQuotaInternal(path, null, namespaceQuota, storagespaceQuota, type);
  }

  /**
   * Set quota for the federation path.
   * @param path Federation path.
   * @param locations Locations of the Federation path.
   * @param namespaceQuota Name space quota.
   * @param storagespaceQuota Storage space quota.
   * @param type StorageType that the space quota is intended to be set on.
   * @throws IOException If the quota system is disabled.
   */
  void setQuotaInternal(String path, List<RemoteLocation> locations,
      long namespaceQuota, long storagespaceQuota, StorageType type)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    // Set quota for current path and its children mount table path.
    if (locations == null) {
      locations = getQuotaRemoteLocations(path);
    }
    if (LOG.isDebugEnabled()) {
      for (RemoteLocation loc : locations) {
        LOG.debug("Set quota for path: nsId: {}, dest: {}.",
            loc.getNameserviceId(), loc.getDest());
      }
    }

    RemoteMethod method = new RemoteMethod("setQuota",
        new Class<?>[] {String.class, long.class, long.class,
            StorageType.class},
        new RemoteParam(), namespaceQuota, storagespaceQuota, type);
    rpcClient.invokeConcurrent(locations, method, false, false);
  }

  /**
   * Get aggregated quota usage for the federation path.
   * @param path Federation path.
   * @return Aggregated quota.
   * @throws IOException If the quota system is disabled.
   */
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    return aggregateQuota(path, getEachQuotaUsage(path));
  }

  /**
   * Get quota usage for the federation path.
   * @param path Federation path.
   * @return quota usage for each remote location.
   * @throws IOException If the quota system is disabled.
   */
  Map<RemoteLocation, QuotaUsage> getEachQuotaUsage(String path)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);
    if (!router.isQuotaEnabled()) {
      throw new IOException("The quota system is disabled in Router.");
    }

    final List<RemoteLocation> quotaLocs = getValidQuotaLocations(path);
    RemoteMethod method = new RemoteMethod("getQuotaUsage",
        new Class<?>[] {String.class}, new RemoteParam());
    Map<RemoteLocation, QuotaUsage> results = rpcClient.invokeConcurrent(
        quotaLocs, method, true, false, QuotaUsage.class);

    return results;
  }

  /**
   * Get global quota for the federation path.
   * @param path Federation path.
   * @return global quota for path.
   * @throws IOException If the quota system is disabled.
   */
  QuotaUsage getGlobalQuota(String path) throws IOException {
    if (!router.isQuotaEnabled()) {
      throw new IOException("The quota system is disabled in Router.");
    }

    long nQuota = HdfsConstants.QUOTA_RESET;
    long sQuota = HdfsConstants.QUOTA_RESET;
    long[] typeQuota = new long[StorageType.values().length];
    eachByStorageType(t -> typeQuota[t.ordinal()] = HdfsConstants.QUOTA_RESET);

    RouterQuotaManager manager = this.router.getQuotaManager();
    TreeMap<String, RouterQuotaUsage> pts =
        manager.getParentsContainingQuota(path);
    Entry<String, RouterQuotaUsage> entry = pts.lastEntry();
    while (entry != null && (nQuota == HdfsConstants.QUOTA_RESET
        || sQuota == HdfsConstants.QUOTA_RESET || orByStorageType(
          t -> typeQuota[t.ordinal()] == HdfsConstants.QUOTA_RESET))) {
      String ppath = entry.getKey();
      QuotaUsage quota = entry.getValue();
      if (nQuota == HdfsConstants.QUOTA_RESET) {
        nQuota = quota.getQuota();
      }
      if (sQuota == HdfsConstants.QUOTA_RESET) {
        sQuota = quota.getSpaceQuota();
      }
      eachByStorageType(t -> {
        if (typeQuota[t.ordinal()] == HdfsConstants.QUOTA_RESET) {
          typeQuota[t.ordinal()] = quota.getTypeQuota(t);
        }
      });
      entry = pts.lowerEntry(ppath);
    }
    return new QuotaUsage.Builder().quota(nQuota).spaceQuota(sQuota)
        .typeQuota(typeQuota).build();
  }

  /**
   * Is the path a mount entry.
   *
   * @param path the path to be checked.
   * @return {@code true} if path is a mount entry; {@code false} otherwise.
   */
  private boolean isMountEntry(String path) {
    return router.getQuotaManager().isMountEntry(path);
  }

  /**
   * Get valid quota remote locations used in {@link #getQuotaUsage(String)}.
   * Differentiate the method {@link #getQuotaRemoteLocations(String)}, this
   * method will do some additional filtering.
   * @param path Federation path.
   * @return List of valid quota remote locations.
   * @throws IOException
   */
  private List<RemoteLocation> getValidQuotaLocations(String path)
      throws IOException {
    final List<RemoteLocation> locations = getQuotaRemoteLocations(path);

    // NameService -> Locations
    ListMultimap<String, RemoteLocation> validLocations =
        ArrayListMultimap.create();

    for (RemoteLocation loc : locations) {
      final String nsId = loc.getNameserviceId();
      final Collection<RemoteLocation> dests = validLocations.get(nsId);

      // Ensure the paths in the same nameservice is different.
      // Do not include parent-child paths.
      boolean isChildPath = false;

      for (RemoteLocation d : dests) {
        if (isParentEntry(loc.getDest(), d.getDest())) {
          isChildPath = true;
          break;
        }
      }

      if (!isChildPath) {
        validLocations.put(nsId, loc);
      }
    }

    return Collections
        .unmodifiableList(new ArrayList<>(validLocations.values()));
  }

  /**
   * Aggregate quota that queried from sub-clusters.
   * @param path Federation path of the results.
   * @param results Quota query result.
   * @return Aggregated Quota.
   */
  QuotaUsage aggregateQuota(String path,
      Map<RemoteLocation, QuotaUsage> results) throws IOException {
    long nsCount = 0;
    long ssCount = 0;
    long[] typeCount = new long[StorageType.values().length];
    long nsQuota = HdfsConstants.QUOTA_RESET;
    long ssQuota = HdfsConstants.QUOTA_RESET;
    long[] typeQuota = new long[StorageType.values().length];
    eachByStorageType(t -> typeQuota[t.ordinal()] = HdfsConstants.QUOTA_RESET);
    boolean hasQuotaUnset = false;
    boolean isMountEntry = isMountEntry(path);

    for (Map.Entry<RemoteLocation, QuotaUsage> entry : results.entrySet()) {
      RemoteLocation loc = entry.getKey();
      QuotaUsage usage = entry.getValue();
      if (isMountEntry) {
        nsCount += usage.getFileAndDirectoryCount();
        ssCount += usage.getSpaceConsumed();
        eachByStorageType(
            t -> typeCount[t.ordinal()] += usage.getTypeConsumed(t));
      } else if (usage != null) {
        // If quota is not set in real FileSystem, the usage
        // value will return -1.
        if (!RouterQuotaManager.isQuotaSet(usage)) {
          hasQuotaUnset = true;
        }
        nsQuota = usage.getQuota();
        ssQuota = usage.getSpaceQuota();
        eachByStorageType(t -> typeQuota[t.ordinal()] = usage.getTypeQuota(t));

        nsCount += usage.getFileAndDirectoryCount();
        ssCount += usage.getSpaceConsumed();
        eachByStorageType(
            t -> typeCount[t.ordinal()] += usage.getTypeConsumed(t));
        LOG.debug("Get quota usage for path: nsId: {}, dest: {},"
                + " nsCount: {}, ssCount: {}, typeCount: {}.",
            loc.getNameserviceId(), loc.getDest(),
            usage.getFileAndDirectoryCount(), usage.getSpaceConsumed(),
            usage.toString(false, true, Arrays.asList(StorageType.values())));
      }
    }

    if (isMountEntry) {
      QuotaUsage quota = getGlobalQuota(path);
      nsQuota = quota.getQuota();
      ssQuota = quota.getSpaceQuota();
      eachByStorageType(t -> typeQuota[t.ordinal()] = quota.getTypeQuota(t));
    }
    QuotaUsage.Builder builder =
        new QuotaUsage.Builder().fileAndDirectoryCount(nsCount)
            .spaceConsumed(ssCount).typeConsumed(typeCount);
    if (hasQuotaUnset) {
      builder.quota(HdfsConstants.QUOTA_RESET)
          .spaceQuota(HdfsConstants.QUOTA_RESET);
      eachByStorageType(t -> builder.typeQuota(t, HdfsConstants.QUOTA_RESET));
    } else {
      builder.quota(nsQuota).spaceQuota(ssQuota);
      eachByStorageType(t -> builder.typeQuota(t, typeQuota[t.ordinal()]));
    }

    return builder.build();
  }

  /**
   * Invoke consumer by each storage type.
   * @param consumer the function consuming the storage type.
   */
  public static void eachByStorageType(Consumer<StorageType> consumer) {
    for (StorageType type : StorageType.values()) {
      consumer.accept(type);
    }
  }

  /**
   * Invoke predicate by each storage type and bitwise inclusive OR the results.
   *
   * @param predicate the function test the storage type.
   * @return true if bitwise OR by all storage type returns true, false otherwise.
   */
  public static boolean orByStorageType(Predicate<StorageType> predicate) {
    boolean res = false;
    for (StorageType type : StorageType.values()) {
      res |= predicate.test(type);
    }
    return res;
  }

  /**
   * Invoke predicate by each storage type and bitwise AND the results.
   *
   * @param predicate the function test the storage type.
   * @return true if bitwise AND by all storage type returns true, false otherwise.
   */
  public static boolean andByStorageType(Predicate<StorageType> predicate) {
    boolean res = false;
    for (StorageType type : StorageType.values()) {
      res &= predicate.test(type);
    }
    return res;
  }

  /**
   * Get all quota remote locations across subclusters under given
   * federation path.
   * @param path Federation path.
   * @return List of quota remote locations.
   * @throws IOException
   */
  private List<RemoteLocation> getQuotaRemoteLocations(String path)
      throws IOException {
    List<RemoteLocation> locations = new ArrayList<>();
    RouterQuotaManager manager = this.router.getQuotaManager();
    if (manager != null) {
      Set<String> childrenPaths = manager.getPaths(path);
      for (String childPath : childrenPaths) {
        locations.addAll(
            rpcServer.getLocationsForPath(childPath, false, false));
      }
    }
    if (locations.size() >= 1) {
      return locations;
    } else {
      locations.addAll(rpcServer.getLocationsForPath(path, false, false));
      return locations;
    }
  }
}
