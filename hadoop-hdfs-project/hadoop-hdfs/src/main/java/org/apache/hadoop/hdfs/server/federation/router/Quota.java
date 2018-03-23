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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * @throws IOException
   */
  public void setQuota(String path, long namespaceQuota,
      long storagespaceQuota, StorageType type) throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    // Set quota for current path and its children mount table path.
    final List<RemoteLocation> locations = getQuotaRemoteLocations(path);
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
   * Get quota usage for the federation path.
   * @param path Federation path.
   * @return Aggregated quota.
   * @throws IOException
   */
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    final List<RemoteLocation> quotaLocs = getValidQuotaLocations(path);
    RemoteMethod method = new RemoteMethod("getQuotaUsage",
        new Class<?>[] {String.class}, new RemoteParam());
    Map<RemoteLocation, QuotaUsage> results = rpcClient.invokeConcurrent(
        quotaLocs, method, true, false, QuotaUsage.class);

    return aggregateQuota(results);
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
    Map<String, List<RemoteLocation>> validLocations = new HashMap<>();
    for (RemoteLocation loc : locations) {
      String nsId = loc.getNameserviceId();
      List<RemoteLocation> dests = validLocations.get(nsId);
      if (dests == null) {
        dests = new LinkedList<>();
        dests.add(loc);
        validLocations.put(nsId, dests);
      } else {
        // Ensure the paths in the same nameservice is different.
        // Don't include parent-child paths.
        boolean isChildPath = false;
        for (RemoteLocation d : dests) {
          if (loc.getDest().startsWith(d.getDest())) {
            isChildPath = true;
            break;
          }
        }

        if (!isChildPath) {
          dests.add(loc);
        }
      }
    }

    List<RemoteLocation> quotaLocs = new LinkedList<>();
    for (List<RemoteLocation> locs : validLocations.values()) {
      quotaLocs.addAll(locs);
    }

    return quotaLocs;
  }

  /**
   * Aggregate quota that queried from sub-clusters.
   * @param results Quota query result.
   * @return Aggregated Quota.
   */
  private QuotaUsage aggregateQuota(Map<RemoteLocation, QuotaUsage> results) {
    long nsCount = 0;
    long ssCount = 0;
    boolean hasQuotaUnSet = false;

    for (Map.Entry<RemoteLocation, QuotaUsage> entry : results.entrySet()) {
      RemoteLocation loc = entry.getKey();
      QuotaUsage usage = entry.getValue();
      if (usage != null) {
        // If quota is not set in real FileSystem, the usage
        // value will return -1.
        if (usage.getQuota() == -1 && usage.getSpaceQuota() == -1) {
          hasQuotaUnSet = true;
        }

        nsCount += usage.getFileAndDirectoryCount();
        ssCount += usage.getSpaceConsumed();
        LOG.debug(
            "Get quota usage for path: nsId: {}, dest: {},"
                + " nsCount: {}, ssCount: {}.",
            loc.getNameserviceId(), loc.getDest(),
            usage.getFileAndDirectoryCount(), usage.getSpaceConsumed());
      }
    }

    QuotaUsage.Builder builder = new QuotaUsage.Builder()
        .fileAndDirectoryCount(nsCount).spaceConsumed(ssCount);
    if (hasQuotaUnSet) {
      builder.quota(HdfsConstants.QUOTA_DONT_SET);
    }

    return builder.build();
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
    List<RemoteLocation> locations = new LinkedList<>();
    RouterQuotaManager manager = this.router.getQuotaManager();
    if (manager != null) {
      Set<String> childrenPaths = manager.getPaths(path);
      for (String childPath : childrenPaths) {
        locations.addAll(rpcServer.getLocationsForPath(childPath, true));
      }
    }

    return locations;
  }
}
