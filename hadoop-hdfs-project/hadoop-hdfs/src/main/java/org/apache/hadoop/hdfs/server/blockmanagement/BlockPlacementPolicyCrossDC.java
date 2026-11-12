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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDatacenterCount;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BlockPlacementPolicy that distributes replicas across multiple datacenters.
 *
 * This policy extends BlockPlacementPolicyDefault to provide the following behavior:
 * 1. Distributes replicas evenly between local and remote datacenters:
 *    - Half of replicas (rounded up) go to local datacenter
 *    - Remaining replicas go to remote datacenter(s)
 *
 * Example with replication factor 3:
 * - Writer at /dc1/rack1
 * - Replica 1: /dc1/rack1 (same as writer, local node)
 * - Replica 2: /dc1/rack2 (same DC, different rack)
 * - Replica 3: /dc2/rack1 (different DC)
 *
 * Example with replication factor 5:
 * - Local DC: 3 replicas (ceil(5/2))
 * - Remote DC: 2 replicas (floor(5/2))
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyCrossDC extends BlockPlacementPolicyDefault {

  public static final Logger LOG = LoggerFactory.getLogger(BlockPlacementPolicyCrossDC.class);

  // Configuration flag to enable async cross-DC write
  private boolean asyncCrossDCEnabled;

  // Preferred datacenter name for replica deletion when writer cannot be determined
  private String preferredDatacenter;
  private final int minDatacenter = 2;

  // Limited sync write: Rate limiter for cross-DC writes
  private Bucket bandwidthBucket;

  // Path-based write mode configuration
  private PathTrie syncWritePaths;
  private PathTrie limitedSyncWritePaths;
  private static final ThreadLocal<String> currentSrcPath = new ThreadLocal<>();

  // Write mode enum for path-based configuration
  private enum WriteMode {
    SYNC,           // Always sync write (all DCs)
    LIMITED_SYNC,   // Bandwidth-based limited sync write
    ASYNC           // Always async write (local DC only)
  }

  /**
   * Path Trie data structure for efficient prefix matching.
   * Optimizes path matching from O(n) to O(path depth).
   * Thread-safe for concurrent reads after initialization.
   */
  private static class PathTrie {
    private final TrieNode root = new TrieNode();

    private static class TrieNode {
      private final Map<String, TrieNode> children = new HashMap<>();
      private boolean isEndOfPath = false;
    }

    /**
     * Insert a path into the trie.
     * @param path Path to insert (e.g., "/foo/bar")
     */
    public void insert(String path) {
      if (path == null || path.isEmpty()) {
        return;
      }

      // Split path into segments, skipping empty segments
      String[] segments = path.split("/");
      TrieNode current = root;

      for (String segment : segments) {
        if (segment.isEmpty()) {
          continue;
        }
        current = current.children.computeIfAbsent(segment, k -> new TrieNode());
      }
      current.isEndOfPath = true;
    }

    /**
     * Check if the given path matches any inserted path (prefix match).
     * For example, if "/foo" is inserted, "/foo/bar" matches.
     * @param path Path to check
     * @return true if path matches
     */
    public boolean matches(String path) {
      if (path == null || path.isEmpty()) {
        return false;
      }

      String[] segments = path.split("/");
      TrieNode current = root;

      for (String segment : segments) {
        if (segment.isEmpty()) {
          continue;
        }

        // If we found a complete configured path, it's a match (prefix match)
        if (current.isEndOfPath) {
          return true;
        }

        // Move to next segment
        current = current.children.get(segment);
        if (current == null) {
          return false;
        }
      }

      // Check if current node is end of configured path (exact match)
      return current.isEndOfPath;
    }

    /**
     * Check if trie is empty.
     * @return true if no paths inserted
     */
    public boolean isEmpty() {
      return root.children.isEmpty();
    }
  }

  @Override
  public void initialize(Configuration conf, FSClusterStats stats, NetworkTopology clusterMap,
      Host2NodesMap host2datanodeMap) {
    if (!(clusterMap instanceof DFSNetworkTopologyWithDatacenterCount)) {
      throw new IllegalArgumentException(
          "Configured cluster topology should be "
              + DFSNetworkTopologyWithDatacenterCount.class.getName());
    }
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    this.asyncCrossDCEnabled =
        conf.getBoolean(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_ASYNC_ENABLED_KEY,
            DFSConfigKeys.DFS_BLOCK_REPLICATOR_ASYNC_CROSS_DC_ENABLED_DEFAULT);
    this.preferredDatacenter =
        conf.get(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_PREFERRED_DATACENTER_KEY,
            DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_PREFERRED_DATACENTER_DEFAULT);

    // Limited sync write: Initialize bandwidth bucket
    // Note: Bandwidth limit is configured in MB to avoid exceeding Bucket4j's
    // maximum refill rate of 1 token/nanosecond
    long bandwidthLimitMB = conf.getLong(
        DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_BANDWIDTH_LIMIT_MB_KEY,
        DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_BANDWIDTH_LIMIT_DEFAULT);
    long refillPeriodSeconds = conf.getLong(
        DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_BANDWIDTH_REFILL_PERIOD_SEC_KEY,
        DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_BANDWIDTH_REFILL_PERIOD_DEFAULT);

    // Create bucket with MB as token unit
    // Using MB instead of bytes keeps refill rate below Bucket4j's 1 token/ns limit
    Bandwidth limit = Bandwidth.builder()
        .capacity(bandwidthLimitMB)  // capacity (tokens in MB)
        .refillGreedy(bandwidthLimitMB, Duration.ofSeconds(refillPeriodSeconds))
        .build();
    this.bandwidthBucket = Bucket.builder().addLimit(limit).build();

    // Path-based write mode configuration: Parse comma-separated paths
    String syncPathsStr = conf.get(
        DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_SYNC_PATHS_KEY,
        DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_SYNC_PATHS_DEFAULT);
    String limitedSyncPathsStr = conf.get(
        DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_LIMITED_SYNC_PATHS_KEY,
        DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_LIMITED_SYNC_PATHS_DEFAULT);

    this.syncWritePaths = parsePaths(syncPathsStr);
    this.limitedSyncWritePaths = parsePaths(limitedSyncPathsStr);

    LOG.info("Async cross-DC write is {}", asyncCrossDCEnabled ? "enabled" : "disabled");
    LOG.info("Limited sync write - bandwidth limit: {} megabytes/sec, refill period: {} sec",
        bandwidthLimitMB, refillPeriodSeconds);
    if (!syncWritePaths.isEmpty()) {
      LOG.info("Sync write paths: {}", syncWritePaths);
    }
    if (!limitedSyncWritePaths.isEmpty()) {
      LOG.info("Limited sync write paths: {}", limitedSyncWritePaths);
    }
    if (preferredDatacenter != null && !preferredDatacenter.isEmpty()) {
      LOG.info("Preferred datacenter for replica deletion: {}", preferredDatacenter);
    }
  }

  /**
   * Parse comma-separated path configuration into a PathTrie.
   * Trims whitespace and filters out empty strings.
   *
   * @param pathsStr Comma-separated path string
   * @return PathTrie containing normalized paths
   */
  private PathTrie parsePaths(String pathsStr) {
    PathTrie trie = new PathTrie();
    if (pathsStr == null || pathsStr.trim().isEmpty()) {
      return trie;
    }

    for (String path : pathsStr.split(",")) {
      path = path.trim();
      if (!path.isEmpty()) {
        // Ensure path starts with /
        if (!path.startsWith("/")) {
          path = "/" + path;
        }
        trie.insert(path);
      }
    }
    return trie;
  }

  /**
   * Determine write mode for the given file path based on configuration.
   * Priority order: sync paths > limited sync paths > default async.
   *
   * @param srcPath Source file path
   * @return WriteMode for this path
   */
  private WriteMode determineWriteMode(String srcPath) {
    if (srcPath == null) {
      return asyncCrossDCEnabled ? WriteMode.ASYNC : WriteMode.SYNC;
    }

    // Priority 1: Check sync write paths
    if (syncWritePaths.matches(srcPath)) {
      return WriteMode.SYNC;
    }

    // Priority 2: Check limited sync write paths
    if (limitedSyncWritePaths.matches(srcPath)) {
      return WriteMode.LIMITED_SYNC;
    }

    // Default: async if enabled, otherwise sync
    return asyncCrossDCEnabled ? WriteMode.ASYNC : WriteMode.SYNC;
  }

  /**
   * Override chooseTarget to capture srcPath in ThreadLocal for path-based write mode decision.
   */
  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas,
      Node writer, List<DatanodeStorageInfo> chosenNodes, boolean returnChosenNodes,
      Set<Node> excludedNodes, long blocksize,
      final BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags) {
    try {
      // Store srcPath in ThreadLocal for use in chooseTargetInOrder
      currentSrcPath.set(srcPath);

      // Delegate to parent implementation
      return super.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, returnChosenNodes,
          excludedNodes, blocksize, storagePolicy, flags);
    } finally {
      // Always clean up ThreadLocal to prevent memory leaks
      currentSrcPath.remove();
    }
  }

  @Override
  protected Node chooseTargetInOrder(int numOfReplicas, Node writer, final Set<Node> excludedNodes,
      final long blocksize, final int maxNodesPerRack, final List<DatanodeStorageInfo> results,
      final boolean avoidStaleNodes, final boolean newBlock,
      EnumMap<StorageType, Integer> storageTypes) throws NotEnoughReplicasException {
    final int numOfResults = results.size();

    // Path-based write mode decision
    boolean asyncWrite = false;
    if (newBlock) {
      String srcPath = currentSrcPath.get();
      WriteMode writeMode = determineWriteMode(srcPath);

      switch (writeMode) {
        case SYNC:
          // Always sync write (all DCs)
          LOG.debug("Path {} matched sync write paths, using sync write (all DCs)", srcPath);
          break;

        case LIMITED_SYNC:
          // Bandwidth-based limited sync write
          // Convert blocksize from bytes to MB for token bucket consumption
          long blocksizeMB = blocksize / (1024 * 1024);
          blocksizeMB = blocksizeMB < 1 ? 1 : blocksizeMB;
          if (!bandwidthBucket.tryConsume(blocksizeMB)) {
            // Bandwidth limit exceeded, use async write to minimize client latency
            asyncWrite = true;
            LOG.debug("Path {} uses limited sync write but bandwidth limit exceeded for block size {} bytes ({} MB), using async write (local DC only)",
                srcPath, blocksize, blocksizeMB);
          } else {
            // Bandwidth available, use sync write for better durability
            LOG.debug("Path {} uses limited sync write and bandwidth available for block size {} bytes ({} MB), using sync write (all DCs)",
                srcPath, blocksize, blocksizeMB);
          }
          break;

        case ASYNC:
          // Always async write (local DC only)
          asyncWrite = true;
          LOG.debug("Path {} uses async write (local DC only)", srcPath);
          break;
      }
    }
    if (numOfResults == 0) {
      DatanodeStorageInfo storageInfo =
          chooseLocalStorage(writer, excludedNodes, blocksize, maxNodesPerRack, results,
              avoidStaleNodes, storageTypes, true);

      writer = (storageInfo != null) ? storageInfo.getDatanodeDescriptor() : null;

      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    final DatanodeDescriptor dn0 = results.get(0).getDatanodeDescriptor();
    if (numOfResults <= 1) {
      if (asyncWrite) {
        chooseLocalRack(dn0, excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes,
            storageTypes);
      } else {
        chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack, results,
            avoidStaleNodes, storageTypes);
      }
      if (--numOfReplicas == 0) {
        if (asyncWrite) {
          filterLocalDatacenterTargets(results, writer);
        }
        return writer;
      }
    }

    String majorityDC = null;
    Map<String, List<DatanodeStorageInfo>> dcToReplicas = new HashMap<>();
    for (DatanodeStorageInfo storage : results) {
      String dc = DFSNetworkTopologyWithDatacenterCount.getDatacenter(storage.getDatanodeDescriptor());
      dcToReplicas.computeIfAbsent(dc, k -> new ArrayList<>()).add(storage);
    }

    if (preferredDatacenter != null && !preferredDatacenter.isEmpty()) {
      if (dcToReplicas.containsKey(preferredDatacenter)) {
        majorityDC = preferredDatacenter;
      } else {
        try {
          chooseRandom(preferredDatacenter, excludedNodes, blocksize, maxNodesPerRack, results,
              avoidStaleNodes, storageTypes);
          dcToReplicas.computeIfAbsent(preferredDatacenter, k -> new ArrayList<>())
              .add(results.get(results.size() - 1));
          if (--numOfReplicas == 0) {
            if (asyncWrite) {
              filterLocalDatacenterTargets(results, writer);
            }
            return writer;
          }
          majorityDC = preferredDatacenter;
        } catch (NotEnoughReplicasException e) {
          LOG.debug("Failed to choose node from preferred datacenters, give up", e);
        }
      }
    }

    if (majorityDC == null) {
      if (writer != null) {
        majorityDC = DFSNetworkTopologyWithDatacenterCount.getDatacenter(writer);
      } else {
        majorityDC = DFSNetworkTopologyWithDatacenterCount.getDatacenter(dn0);
      }
    }

    if (numOfResults <= 2) {
      final DatanodeDescriptor dn1 = results.get(1).getDatanodeDescriptor();
      // isOnSameRack : /dc1/r1, /dc1/r2 same parent == same datacenter
      if (DFSNetworkTopologyWithDatacenterCount.getDatacenter(dn0).equals(DFSNetworkTopologyWithDatacenterCount.getDatacenter(dn1))) {
        chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack, results,
            avoidStaleNodes, storageTypes);
      } else {
        chooseLocalRack(dcToReplicas.get(majorityDC).get(0).getDatanodeDescriptor(), excludedNodes,
            blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
      }
      if (--numOfReplicas == 0) {
        if (asyncWrite) {
          filterLocalDatacenterTargets(results, writer);
        }
        return writer;
      }
      DatanodeStorageInfo chosen = results.get(results.size() - 1);
      dcToReplicas.computeIfAbsent(DFSNetworkTopologyWithDatacenterCount.getDatacenter(chosen.getDatanodeDescriptor()), k -> new ArrayList<>())
          .add(chosen);
    }

    int expectedNumOfReplicas = results.size() + numOfReplicas;
    int targetMajorityDC = (expectedNumOfReplicas + 1) / 2;

    int majorBlockCountToAdd = targetMajorityDC - dcToReplicas.get(majorityDC).size();
    if (majorBlockCountToAdd < 0) {
      majorBlockCountToAdd = 0;
    }
    int oldNumOfReplicas = results.size();
    try {
      DatanodeStorageInfo majorityDCStorageInfo = dcToReplicas.get(majorityDC).get(0);
      chooseRandom(majorBlockCountToAdd, majorityDC, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes);
      chooseRemoteRack(numOfReplicas - majorBlockCountToAdd,
          majorityDCStorageInfo.getDatanodeDescriptor(), excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      chooseRandom(numOfReplicas - (results.size() - oldNumOfReplicas), NodeBase.ROOT,
          excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }

    // Filter to local datacenter targets if async cross-DC write is enabled
    if (asyncWrite) {
      filterLocalDatacenterTargets(results, writer);
    }

    return writer;
  }

  @Override
  protected DatanodeStorageInfo chooseLocalRack(Node localMachine, Set<Node> excludedNodes,
      long blocksize, int maxNodesPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes, EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize, maxNodesPerRack, results,
          avoidStaleNodes, storageTypes);
    }

    try {
      final String scope = localMachine.getNetworkLocation();
      return chooseRandom(scope, excludedNodes, blocksize, maxNodesPerRack, results,
          avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e1) {
      final String scope = DFSNetworkTopologyWithDatacenterCount.getDatacenter(localMachine);
      try {
        return chooseRandom(scope, excludedNodes, blocksize, maxNodesPerRack, results,
            avoidStaleNodes, storageTypes);
      } catch (NotEnoughReplicasException e2) {
        return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize, maxNodesPerRack, results,
            avoidStaleNodes, storageTypes);
      }
    }
  }

  /** choose node in different datacenter */
  @Override
  protected void chooseRemoteRack(int numOfReplicas, DatanodeDescriptor localMachine,
      Set<Node> excludedNodes, long blocksize, int maxReplicasPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes) throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();

    final String datacenterLocation = DFSNetworkTopologyWithDatacenterCount.getDatacenter(localMachine);
    try {
      // randomly choose from remote racks
      chooseRandom(numOfReplicas, "~" + datacenterLocation, excludedNodes, blocksize,
          maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      // fall back to the local rack
      chooseRandom(numOfReplicas - (results.size() - oldNumOfReplicas), datacenterLocation,
          excludedNodes, blocksize, maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs, int numberOfReplicas) {
    if (locs == null)
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    if (!clusterMap.hasClusterEverBeenMultiRack()) {
      // only one rack
      return new BlockPlacementStatusDefault(1, 1, 1);
    }

    Map<String, Integer> dcToReplicaCount = new HashMap<>();
    for (DatanodeInfo dn : locs) {
      String dc = DFSNetworkTopologyWithDatacenterCount.getDatacenter(dn);
      dcToReplicaCount.put(dc, dcToReplicaCount.getOrDefault(dc, 0) + 1);
    }
    int neededReplicaCount = 0;

    DFSNetworkTopologyWithDatacenterCount clusterMapDatacenter =
        (DFSNetworkTopologyWithDatacenterCount) clusterMap;
    int numOfNodesInPreferredDatacenter =
        clusterMapDatacenter.getNumOfNodesInDatacenter(preferredDatacenter);
    if (preferredDatacenter != null && !preferredDatacenter.isEmpty()
        && numOfNodesInPreferredDatacenter > 0) {
      int shouldExists = Math.min((numberOfReplicas + 1) / 2, numOfNodesInPreferredDatacenter);
      int diff = shouldExists - dcToReplicaCount.getOrDefault(preferredDatacenter, 0);
      if (diff > 0) {
        neededReplicaCount += diff;
      }
    }

    int totalDatacenters = clusterMapDatacenter.getNumOfNonEmptyDatacenters();

    return new BlockPlacementStatusWithCrossDC(dcToReplicaCount.size(), minDatacenter,
        neededReplicaCount, totalDatacenters);
  }

  /**
   * Choose replicas to delete while maintaining datacenter diversity.
   */
  @Override
  public List<DatanodeStorageInfo> chooseReplicasToDelete(
      Collection<DatanodeStorageInfo> availableReplicas,
      Collection<DatanodeStorageInfo> delCandidates, int expectedNumOfReplicas,
      List<StorageType> excessTypes, DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint) {

    // If only one datacenter exists, use parent's implementation
    if (!clusterMap.hasClusterEverBeenMultiRack()) {
      return super.chooseReplicasToDelete(availableReplicas, delCandidates, expectedNumOfReplicas,
          excessTypes, addedNode, delNodeHint);
    }

    int numToDelete = delCandidates.size() - expectedNumOfReplicas;
    if (numToDelete <= 0) {
      return new ArrayList<>();
    }

    Map<String, List<DatanodeStorageInfo>> dcToReplicas = new HashMap<>();
    for (DatanodeStorageInfo storage : delCandidates) {
      String dc = DFSNetworkTopologyWithDatacenterCount.getDatacenter(storage.getDatanodeDescriptor());
      dcToReplicas.computeIfAbsent(dc, k -> new ArrayList<>()).add(storage);
    }

    int targetMajorityDC = (expectedNumOfReplicas + 1) / 2;
    String majorityDC = null;

    if (dcToReplicas.isEmpty()) {
      LOG.warn("No replicas found in deletion candidates, cannot proceed with DC-aware deletion");
      return new ArrayList<>();
    }

    if (preferredDatacenter != null && !preferredDatacenter.isEmpty() && dcToReplicas.containsKey(
        preferredDatacenter)) {
      majorityDC = preferredDatacenter;
      LOG.debug("Using configured preferred datacenter as majority: {}", majorityDC);
    } else {
      // Fall back to datacenter with most replicas
      int maxCount = 0;
      for (Map.Entry<String, List<DatanodeStorageInfo>> entry : dcToReplicas.entrySet()) {
        if (entry.getValue().size() > maxCount) {
          maxCount = entry.getValue().size();
          majorityDC = entry.getKey();
        }
      }
      LOG.debug("Using datacenter with most replicas as majority: {}", majorityDC);
    }

    // Use parent's splitNodesWithRack to group by rack
    Map<String, List<DatanodeStorageInfo>> rackMap = new HashMap<>();
    List<DatanodeStorageInfo> moreThanOne = new ArrayList<>();
    List<DatanodeStorageInfo> exactlyOne = new ArrayList<>();
    splitNodesWithRack(availableReplicas, delCandidates, rackMap, moreThanOne, exactlyOne);

    // Select replicas to delete, prioritizing by datacenter
    List<DatanodeStorageInfo> excessReplicas = new ArrayList<>();
    Set<DatanodeStorageInfo> alreadyChosen = new HashSet<>();

    // 1. First, delete from major datacenter
    if (dcToReplicas.containsKey(majorityDC)) {
      int count = dcToReplicas.get(majorityDC).size();
      for (int i = 0; i < count - targetMajorityDC && excessReplicas.size() < numToDelete; i++) {
        DatanodeStorageInfo chosen =
            chooseReplicaToDeleteFromDC(majorityDC, moreThanOne, exactlyOne, excessTypes, rackMap,
                alreadyChosen);
        if (chosen != null) {
          excessReplicas.add(chosen);
          alreadyChosen.add(chosen);
          moreThanOne.remove(chosen);
          exactlyOne.remove(chosen);
        }
      }
    }

    // 2. Second, delete among other datacenters
    for (Map.Entry<String, List<DatanodeStorageInfo>> entry : dcToReplicas.entrySet()) {
      if (excessReplicas.size() >= numToDelete) {
        break;
      }
      if (entry.getKey().equals(majorityDC)) {
        continue;
      }
      String targetDC = entry.getKey();
      for (int i = 0; i < entry.getValue().size() && excessReplicas.size() < numToDelete; i++) {
        DatanodeStorageInfo chosen =
            chooseReplicaToDeleteFromDC(targetDC, moreThanOne, exactlyOne, excessTypes, rackMap,
                alreadyChosen);

        if (chosen != null) {
          excessReplicas.add(chosen);
          alreadyChosen.add(chosen);
          moreThanOne.remove(chosen);
          exactlyOne.remove(chosen);
        }
      }
    }

    // If still need to delete more (e.g., couldn't find enough in target DCs),
    // use parent's logic for remaining deletions
    while (excessReplicas.size() < numToDelete) {
      DatanodeStorageInfo chosen =
          chooseReplicaToDelete(moreThanOne, exactlyOne, excessTypes, rackMap);

      if (chosen != null && !alreadyChosen.contains(chosen)) {
        excessReplicas.add(chosen);
        alreadyChosen.add(chosen);
        moreThanOne.remove(chosen);
        exactlyOne.remove(chosen);
      } else {
        // No more valid candidates
        break;
      }
    }

    return excessReplicas;
  }

  /**
   * Choose a single replica to delete from a specific datacenter.
   * Prioritizes replicas on racks with multiple replicas to maintain rack diversity.
   *
   * @param targetDC The datacenter to delete from
   * @param moreThanOne Replicas on racks with multiple replicas
   * @param exactlyOne Replicas on racks with exactly one replica
   * @param excessTypes Excess storage types according to storage policy
   * @param rackMap Map of rack to replicas
   * @param alreadyChosen Replicas already chosen for deletion
   * @return The replica to delete, or null if none found
   */
  private DatanodeStorageInfo chooseReplicaToDeleteFromDC(String targetDC,
      Collection<DatanodeStorageInfo> moreThanOne, Collection<DatanodeStorageInfo> exactlyOne,
      List<StorageType> excessTypes, Map<String, List<DatanodeStorageInfo>> rackMap,
      Set<DatanodeStorageInfo> alreadyChosen) {

    // First, try to find a replica in targetDC from moreThanOne
    // (better for maintaining rack diversity)
    for (DatanodeStorageInfo storage : moreThanOne) {
      if (alreadyChosen.contains(storage)) {
        continue;
      }
      String dc = DFSNetworkTopologyWithDatacenterCount.getDatacenter(storage.getDatanodeDescriptor());
      if (dc.equals(targetDC) && (excessTypes == null || excessTypes.isEmpty()
          || excessTypes.contains(storage.getStorageType()))) {
        return storage;
      }
    }

    // If not found in moreThanOne, try exactlyOne
    for (DatanodeStorageInfo storage : exactlyOne) {
      if (alreadyChosen.contains(storage)) {
        continue;
      }
      String dc = DFSNetworkTopologyWithDatacenterCount.getDatacenter(storage.getDatanodeDescriptor());
      if (dc.equals(targetDC) && (excessTypes == null || excessTypes.isEmpty()
          || excessTypes.contains(storage.getStorageType()))) {
        return storage;
      }
    }

    return null;
  }

  /**
   * Filter targets to only include those in the writer's datacenter.
   * Returns up to localDCReplicas datanodes from the same datacenter as the writer.
   *
   * @param results All selected datanodes
   * @param writer Client node
   */
  private void filterLocalDatacenterTargets(List<DatanodeStorageInfo> results, Node writer) {

    String writerDatacenter = DFSNetworkTopologyWithDatacenterCount.getDatacenter(writer);
    List<DatanodeStorageInfo> localTargets = new ArrayList<>();
    int localDCReplicas = (results.size() + 1) / 2;

    // Collect datanodes in the same datacenter as writer
    for (DatanodeStorageInfo target : results) {
      String targetDatacenter = DFSNetworkTopologyWithDatacenterCount.getDatacenter(target.getDatanodeDescriptor());

      if (writerDatacenter.equals(targetDatacenter)) {
        localTargets.add(target);

        // Stop when we have enough local replicas
        if (localTargets.size() >= localDCReplicas) {
          break;
        }
      }
    }

    // Fallback: if not enough local targets, add more from other datacenters
    if (localTargets.size() < localDCReplicas) {
      LOG.warn("Not enough local datacenter targets. Expected: {}, Found: {}. "
          + "Adding targets from remote datacenters.", localDCReplicas, localTargets.size());

      for (DatanodeStorageInfo target : results) {
        if (!localTargets.contains(target)) {
          localTargets.add(target);
          if (localTargets.size() >= localDCReplicas) {
            break;
          }
        }
      }
    }
    results.clear();
    results.addAll(localTargets);
  }
}
