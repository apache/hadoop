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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYSTORAGETYPE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYSTORAGETYPE_KEY;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * The class is responsible for choosing the desired number of targets
 * for placing block replicas.
 * The replica placement strategy is that if the writer is on a datanode,
 * the 1st replica is placed on the local machine by default
 * (By passing the {@link org.apache.hadoop.fs.CreateFlag#NO_LOCAL_WRITE} flag
 * the client can request not to put a block replica on the local datanode.
 * Subsequent replicas will still follow default block placement policy.).
 * If the writer is not on a datanode, the 1st replica is placed on a random
 * node.
 * The 2nd replica is placed on a datanode that is on a different rack.
 * The 3rd replica is placed on a datanode which is on a different node of the
 * rack as the second replica.
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyDefault extends BlockPlacementPolicy {

  private static final String enableDebugLogging =
      "For more information, please enable DEBUG log level on "
          + BlockPlacementPolicy.class.getName() + " and "
          + NetworkTopology.class.getName();

  private static final ThreadLocal<StringBuilder> debugLoggingBuilder
      = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  private static final ThreadLocal<HashMap<NodeNotChosenReason, Integer>>
      CHOOSE_RANDOM_REASONS = ThreadLocal
      .withInitial(() -> new HashMap<NodeNotChosenReason, Integer>());

  private static final BlockPlacementStatus ONE_RACK_PLACEMENT =
      new BlockPlacementStatusDefault(1, 1, 1);

  private enum NodeNotChosenReason {
    NOT_IN_SERVICE("the node is not in service"),
    NODE_STALE("the node is stale"),
    NODE_TOO_BUSY("the node is too busy"),
    NODE_TOO_BUSY_BY_VOLUME("the node is too busy based on volume load"),
    TOO_MANY_NODES_ON_RACK("the rack has too many chosen nodes"),
    NOT_ENOUGH_STORAGE_SPACE("not enough storage space to place the block"),
    NO_REQUIRED_STORAGE_TYPE("required storage types are unavailable"),
    NODE_SLOW("the node is too slow");

    private final String text;

    NodeNotChosenReason(final String logText) {
      text = logText;
    }

    private String getText() {
      return text;
    }
  }

  protected boolean considerLoad;
  private boolean considerLoadByStorageType;
  protected double considerLoadFactor;
  private boolean considerLoadByVolume = false;
  private boolean preferLocalNode;
  private boolean dataNodePeerStatsEnabled;
  private volatile boolean excludeSlowNodesEnabled;
  protected NetworkTopology clusterMap;
  protected Host2NodesMap host2datanodeMap;
  private FSClusterStats stats;
  protected long heartbeatInterval;   // interval for DataNode heartbeats
  private long staleInterval;   // interval used to identify stale DataNodes
  private volatile int minBlocksForWrite; // minimum number of blocks required for write operations.

  /**
   * A miss of that many heartbeats is tolerated for replica deletion policy.
   */
  protected int tolerateHeartbeatMultiplier;

  protected BlockPlacementPolicyDefault() {
  }
    
  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
                         NetworkTopology clusterMap, 
                         Host2NodesMap host2datanodeMap) {
    this.considerLoad = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_DEFAULT);
    this.considerLoadByStorageType = conf.getBoolean(
        DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYSTORAGETYPE_KEY,
        DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYSTORAGETYPE_DEFAULT);
    this.considerLoadFactor = conf.getDouble(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR,
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR_DEFAULT);
    this.considerLoadByVolume = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYVOLUME_KEY,
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYVOLUME_DEFAULT
    );
    this.stats = stats;
    this.clusterMap = clusterMap;
    this.host2datanodeMap = host2datanodeMap;
    this.heartbeatInterval = conf.getTimeDuration(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    this.tolerateHeartbeatMultiplier = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY,
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT);
    this.staleInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
    this.preferLocalNode = conf.getBoolean(
        DFSConfigKeys.
            DFS_NAMENODE_BLOCKPLACEMENTPOLICY_DEFAULT_PREFER_LOCAL_NODE_KEY,
        DFSConfigKeys.
            DFS_NAMENODE_BLOCKPLACEMENTPOLICY_DEFAULT_PREFER_LOCAL_NODE_DEFAULT);
    this.dataNodePeerStatsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY,
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_DEFAULT);
    this.excludeSlowNodesEnabled = conf.getBoolean(
        DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY,
        DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_DEFAULT);
    this.minBlocksForWrite = conf.getInt(
        DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY,
        DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_DEFAULT);
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    Node writer,
                                    List<DatanodeStorageInfo> chosenNodes,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blocksize,
                                    final BlockStoragePolicy storagePolicy,
                                    EnumSet<AddBlockFlag> flags) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, returnChosenNodes,
        excludedNodes, blocksize, storagePolicy, flags, null);
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas,
      Node writer, List<DatanodeStorageInfo> chosen, boolean returnChosenNodes,
      Set<Node> excludedNodes, long blocksize, BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags, EnumMap<StorageType, Integer> storageTypes) {
    return chooseTarget(numOfReplicas, writer, chosen, returnChosenNodes,
        excludedNodes, blocksize, storagePolicy, flags, storageTypes);
  }

  @Override
  DatanodeStorageInfo[] chooseTarget(String src,
      int numOfReplicas,
      Node writer,
      Set<Node> excludedNodes,
      long blocksize,
      List<DatanodeDescriptor> favoredNodes,
      BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags) {
    try {
      if (favoredNodes == null || favoredNodes.size() == 0) {
        // Favored nodes not specified, fall back to regular block placement.
        return chooseTarget(src, numOfReplicas, writer,
            new ArrayList<DatanodeStorageInfo>(numOfReplicas), false, 
            excludedNodes, blocksize, storagePolicy, flags);
      }

      Set<Node> favoriteAndExcludedNodes = excludedNodes == null ?
          new HashSet<Node>() : new HashSet<>(excludedNodes);
      final List<StorageType> requiredStorageTypes = storagePolicy
          .chooseStorageTypes((short)numOfReplicas);
      final EnumMap<StorageType, Integer> storageTypes =
          getRequiredStorageTypes(requiredStorageTypes);

      // Choose favored nodes
      List<DatanodeStorageInfo> results = new ArrayList<>();
      boolean avoidStaleNodes = stats != null
          && stats.isAvoidingStaleDataNodesForWrite();

      int maxNodesAndReplicas[] = getMaxNodesPerRack(0, numOfReplicas);
      numOfReplicas = maxNodesAndReplicas[0];
      int maxNodesPerRack = maxNodesAndReplicas[1];

      chooseFavouredNodes(src, numOfReplicas, favoredNodes,
          favoriteAndExcludedNodes, blocksize, maxNodesPerRack, results,
          avoidStaleNodes, storageTypes);

      if (results.size() < numOfReplicas) {
        // Not enough favored nodes, choose other nodes, based on block
        // placement policy (HDFS-9393).
        numOfReplicas -= results.size();
        for (DatanodeStorageInfo storage : results) {
          // add localMachine and related nodes to favoriteAndExcludedNodes
          addToExcludedNodes(storage.getDatanodeDescriptor(),
              favoriteAndExcludedNodes);
        }
        DatanodeStorageInfo[] remainingTargets =
            chooseTarget(src, numOfReplicas, writer,
                new ArrayList<DatanodeStorageInfo>(numOfReplicas), false,
                favoriteAndExcludedNodes, blocksize, storagePolicy, flags,
                storageTypes);
        for (int i = 0; i < remainingTargets.length; i++) {
          results.add(remainingTargets[i]);
        }
      }
      return getPipeline(writer,
          results.toArray(new DatanodeStorageInfo[results.size()]));
    } catch (NotEnoughReplicasException nr) {
      LOG.debug("Failed to choose with favored nodes (={}), disregard favored nodes hint and retry",
          favoredNodes, nr);
      // Fall back to regular block placement disregarding favored nodes hint
      return chooseTarget(src, numOfReplicas, writer, 
          new ArrayList<DatanodeStorageInfo>(numOfReplicas), false, 
          excludedNodes, blocksize, storagePolicy, flags);
    }
  }

  protected void chooseFavouredNodes(String src, int numOfReplicas,
      List<DatanodeDescriptor> favoredNodes,
      Set<Node> favoriteAndExcludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    for (int i = 0; i < favoredNodes.size() && results.size() < numOfReplicas;
        i++) {
      DatanodeDescriptor favoredNode = favoredNodes.get(i);
      // Choose a single node which is local to favoredNode.
      // 'results' is updated within chooseLocalNode
      final DatanodeStorageInfo target = chooseLocalOrFavoredStorage(
          favoredNode, true, favoriteAndExcludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);

      if (target == null) {
        LOG.warn("Could not find a target for file " + src
            + " with favored node " + favoredNode);
        continue;
      }
      favoriteAndExcludedNodes.add(target.getDatanodeDescriptor());
    }
  }

  /** This is the implementation. */
  private DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
                                    Node writer,
                                    List<DatanodeStorageInfo> chosenStorage,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blocksize,
                                    final BlockStoragePolicy storagePolicy,
                                    EnumSet<AddBlockFlag> addBlockFlags,
                                    EnumMap<StorageType, Integer> sTypes) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return DatanodeStorageInfo.EMPTY_ARRAY;
    }
      
    if (excludedNodes == null) {
      excludedNodes = new HashSet<>();
    }
     
    int[] result = getMaxNodesPerRack(chosenStorage.size(), numOfReplicas);
    numOfReplicas = result[0];
    int maxNodesPerRack = result[1];
      
    for (DatanodeStorageInfo storage : chosenStorage) {
      // add localMachine and related nodes to excludedNodes
      addToExcludedNodes(storage.getDatanodeDescriptor(), excludedNodes);
    }

    List<DatanodeStorageInfo> results = null;
    Node localNode = null;
    boolean avoidStaleNodes = (stats != null
        && stats.isAvoidingStaleDataNodesForWrite());
    boolean avoidLocalRack = (addBlockFlags != null
        && addBlockFlags.contains(AddBlockFlag.NO_LOCAL_RACK) && writer != null
        && clusterMap.getNumOfNonEmptyRacks() > 2);
    boolean avoidLocalNode = (addBlockFlags != null
        && addBlockFlags.contains(AddBlockFlag.NO_LOCAL_WRITE)
        && writer != null
        && !excludedNodes.contains(writer));
    // Attempt to exclude local rack if the client suggests so. If no enough
    // nodes can be obtained or number of racks are less than three, it falls
    // back to the default block placement
    // policy.
    if (avoidLocalRack) {
      results = new ArrayList<>(chosenStorage);
      Set<Node> excludedNodeCopy = new HashSet<>(excludedNodes);
      excludedNodeCopy
          .addAll(clusterMap.getLeaves(writer.getNetworkLocation()));

      localNode = chooseTarget(numOfReplicas, writer, excludedNodeCopy,
          blocksize, maxNodesPerRack, results, avoidStaleNodes, storagePolicy,
          EnumSet.noneOf(StorageType.class), results.isEmpty(), sTypes);
      if (results.size() < numOfReplicas) {
        // not enough nodes; discard results and fall back
        results = null;
      }
    }
    // Attempt to exclude local node if the client suggests so. If no enough
    // nodes can be obtained, it falls back to the default block placement
    // policy.
    if (avoidLocalNode && results == null) {
      results = new ArrayList<>(chosenStorage);
      Set<Node> excludedNodeCopy = new HashSet<>(excludedNodes);
      if (writer != null) {
        excludedNodeCopy.add(writer);
      }
      localNode = chooseTarget(numOfReplicas, writer,
          excludedNodeCopy, blocksize, maxNodesPerRack, results,
          avoidStaleNodes, storagePolicy,
          EnumSet.noneOf(StorageType.class), results.isEmpty(), sTypes);
      if (results.size() < numOfReplicas) {
        // not enough nodes; discard results and fall back
        results = null;
      }
    }
    if (results == null) {
      results = new ArrayList<>(chosenStorage);
      localNode = chooseTarget(numOfReplicas, writer, excludedNodes,
          blocksize, maxNodesPerRack, results, avoidStaleNodes,
          storagePolicy, EnumSet.noneOf(StorageType.class), results.isEmpty(),
          sTypes);
    }

    if (!returnChosenNodes) {  
      results.removeAll(chosenStorage);
    }
      
    // sorting nodes to form a pipeline
    return getPipeline(
        (writer != null && writer instanceof DatanodeDescriptor) ? writer
            : localNode,
        results.toArray(new DatanodeStorageInfo[results.size()]));
  }

  /**
   * Calculate the maximum number of replicas to allocate per rack. It also
   * limits the total number of replicas to the total number of nodes in the
   * cluster. Caller should adjust the replica count to the return value.
   *
   * @param numOfChosen The number of already chosen nodes.
   * @param numOfReplicas The number of additional nodes to allocate.
   * @return integer array. Index 0: The number of nodes allowed to allocate
   *         in addition to already chosen nodes.
   *         Index 1: The maximum allowed number of nodes per rack. This
   *         is independent of the number of chosen nodes, as it is calculated
   *         using the target number of replicas.
   */
  protected int[] getMaxNodesPerRack(int numOfChosen, int numOfReplicas) {
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = numOfChosen + numOfReplicas;
    if (totalNumOfReplicas > clusterSize) {
      numOfReplicas -= (totalNumOfReplicas-clusterSize);
      totalNumOfReplicas = clusterSize;
    }
    // No calculation needed when there is only one rack or picking one node.
    int numOfRacks = clusterMap.getNumOfNonEmptyRacks();
    // HDFS-14527 return default when numOfRacks = 0 to avoid
    // ArithmeticException when calc maxNodesPerRack at following logic.
    if (numOfRacks <= 1 || totalNumOfReplicas <= 1) {
      return new int[] {numOfReplicas, totalNumOfReplicas};
    }

    int maxNodesPerRack = (totalNumOfReplicas-1)/numOfRacks + 2;
    // At this point, there are more than one racks and more than one replicas
    // to store. Avoid all replicas being in the same rack.
    //
    // maxNodesPerRack has the following properties at this stage.
    //   1) maxNodesPerRack >= 2
    //   2) (maxNodesPerRack-1) * numOfRacks > totalNumOfReplicas
    //          when numOfRacks > 1
    //
    // Thus, the following adjustment will still result in a value that forces
    // multi-rack allocation and gives enough number of total nodes.
    if (maxNodesPerRack == totalNumOfReplicas) {
      maxNodesPerRack--;
    }
    return new int[] {numOfReplicas, maxNodesPerRack};
  }

  private EnumMap<StorageType, Integer> getRequiredStorageTypes(
      List<StorageType> types) {
    EnumMap<StorageType, Integer> map = new EnumMap<>(StorageType.class);
    for (StorageType type : types) {
      if (!map.containsKey(type)) {
        map.put(type, 1);
      } else {
        int num = map.get(type);
        map.put(type, num + 1);
      }
    }
    return map;
  }

  /**
   * choose <i>numOfReplicas</i> from all data nodes
   * @param numOfReplicas additional number of replicas wanted
   * @param writer the writer's machine, could be a non-DatanodeDescriptor node
   * @param excludedNodes datanodes that should not be considered as targets
   * @param blocksize size of the data to be written
   * @param maxNodesPerRack max nodes allowed per rack
   * @param results the target nodes already chosen
   * @param avoidStaleNodes avoid stale nodes in replica choosing
   * @param storageTypes storage type to be considered for target
   * @return local node of writer (not chosen node)
   */
  private Node chooseTarget(final int numOfReplicas,
                            Node writer,
                            final Set<Node> excludedNodes,
                            final long blocksize,
                            final int maxNodesPerRack,
                            final List<DatanodeStorageInfo> results,
                            final boolean avoidStaleNodes,
                            final BlockStoragePolicy storagePolicy,
                            final EnumSet<StorageType> unavailableStorages,
                            final boolean newBlock,
                            EnumMap<StorageType, Integer> storageTypes) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return (writer instanceof DatanodeDescriptor) ? writer : null;
    }
    final int numOfResults = results.size();
    final int totalReplicasExpected = numOfReplicas + numOfResults;
    if ((writer == null || !(writer instanceof DatanodeDescriptor)) && !newBlock) {
      writer = results.get(0).getDatanodeDescriptor();
    }

    // Keep a copy of original excludedNodes
    final Set<Node> oldExcludedNodes = new HashSet<>(excludedNodes);

    // choose storage types; use fallbacks for unavailable storages
    final List<StorageType> requiredStorageTypes = storagePolicy
        .chooseStorageTypes((short) totalReplicasExpected,
            DatanodeStorageInfo.toStorageTypes(results),
            unavailableStorages, newBlock);
    if (storageTypes == null) {
      storageTypes = getRequiredStorageTypes(requiredStorageTypes);
    }
    LOG.trace("storageTypes={}", storageTypes);

    try {
      if (requiredStorageTypes.size() == 0) {
        throw new NotEnoughReplicasException(
            "All required storage types are unavailable: "
            + " unavailableStorages=" + unavailableStorages
            + ", storagePolicy=" + storagePolicy);
      }
      writer = chooseTargetInOrder(numOfReplicas, writer, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, newBlock, storageTypes);
    } catch (NotEnoughReplicasException e) {
      final String message = "Failed to place enough replicas, still in need of "
          + (totalReplicasExpected - results.size()) + " to reach "
          + totalReplicasExpected
          + " (unavailableStorages=" + unavailableStorages
          + ", storagePolicy=" + storagePolicy
          + ", newBlock=" + newBlock + ")";

      LOG.trace(message, e);
      LOG.warn(message + " " + e.getMessage());

      if (avoidStaleNodes) {
        // Retry chooseTarget again, this time not avoiding stale nodes.

        // excludedNodes contains the initial excludedNodes and nodes that were
        // not chosen because they were stale, decommissioned, etc.
        // We need to additionally exclude the nodes that were added to the 
        // result list in the successful calls to choose*() above.
        for (DatanodeStorageInfo resultStorage : results) {
          addToExcludedNodes(resultStorage.getDatanodeDescriptor(), oldExcludedNodes);
        }
        // Set newNumOfReplicas, since it can get out of sync with the result list
        // if the NotEnoughReplicasException was thrown in chooseRandom().
        int newNumOfReplicas = totalReplicasExpected - results.size();
        return chooseTarget(newNumOfReplicas, writer, oldExcludedNodes, blocksize,
            maxNodesPerRack, results, false, storagePolicy, unavailableStorages,
            newBlock, null);
      }

      boolean retry = false;
      // simply add all the remaining types into unavailableStorages and give
      // another try. No best effort is guaranteed here.
      for (StorageType type : storageTypes.keySet()) {
        if (!unavailableStorages.contains(type)) {
          unavailableStorages.add(type);
          retry = true;
        }
      }
      if (retry) {
        for (DatanodeStorageInfo resultStorage : results) {
          addToExcludedNodes(resultStorage.getDatanodeDescriptor(),
              oldExcludedNodes);
        }
        int newNumOfReplicas = totalReplicasExpected - results.size();
        return chooseTarget(newNumOfReplicas, writer, oldExcludedNodes, blocksize,
            maxNodesPerRack, results, false, storagePolicy, unavailableStorages,
            newBlock, null);
      }
    }
    return writer;
  }

  protected Node chooseTargetInOrder(int numOfReplicas, 
                                 Node writer,
                                 final Set<Node> excludedNodes,
                                 final long blocksize,
                                 final int maxNodesPerRack,
                                 final List<DatanodeStorageInfo> results,
                                 final boolean avoidStaleNodes,
                                 final boolean newBlock,
                                 EnumMap<StorageType, Integer> storageTypes)
                                 throws NotEnoughReplicasException {
    final int numOfResults = results.size();
    if (numOfResults == 0) {
      DatanodeStorageInfo storageInfo = chooseLocalStorage(writer,
          excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes,
          storageTypes, true);

      writer = (storageInfo != null) ? storageInfo.getDatanodeDescriptor()
                                     : null;

      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    final DatanodeDescriptor dn0 = results.get(0).getDatanodeDescriptor();
    if (numOfResults <= 1) {
      chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes);
      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    if (numOfResults <= 2) {
      final DatanodeDescriptor dn1 = results.get(1).getDatanodeDescriptor();
      if (clusterMap.isOnSameRack(dn0, dn1)) {
        chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      } else if (newBlock){
        chooseLocalRack(dn1, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      } else {
        chooseLocalRack(writer, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      }
      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    return writer;
  }

  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    return chooseLocalOrFavoredStorage(localMachine, false,
        excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes,
        storageTypes);
  }

  /**
   * Choose storage of local or favored node.
   * @param localOrFavoredNode local or favored node
   * @param isFavoredNode if target node is favored node
   * @param excludedNodes datanodes that should not be considered as targets
   * @param blocksize size of the data to be written
   * @param maxNodesPerRack max nodes allowed per rack
   * @param results the target nodes already chosen
   * @param avoidStaleNodes avoid stale nodes in replica choosing
   * @param storageTypes storage type to be considered for target
   * @return storage of local or favored node (not chosen node)
   * @throws NotEnoughReplicasException
   */
  protected DatanodeStorageInfo chooseLocalOrFavoredStorage(
      Node localOrFavoredNode, boolean isFavoredNode, Set<Node> excludedNodes,
      long blocksize, int maxNodesPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes, EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
    if (localOrFavoredNode == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    if ((preferLocalNode || isFavoredNode)
        && localOrFavoredNode instanceof DatanodeDescriptor
        && clusterMap.contains(localOrFavoredNode)) {
      DatanodeDescriptor localDatanode =
          (DatanodeDescriptor) localOrFavoredNode;
      // otherwise try local machine first
      if (excludedNodes.add(localOrFavoredNode) // was not in the excluded list
          && isGoodDatanode(localDatanode, maxNodesPerRack, false,
              results, avoidStaleNodes)) {
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
            .entrySet().iterator(); iter.hasNext(); ) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          DatanodeStorageInfo localStorage = chooseStorage4Block(
              localDatanode, blocksize, results, entry.getKey());
          if (localStorage != null) {
            // add node and related nodes to excludedNode
            addToExcludedNodes(localDatanode, excludedNodes);
            int num = entry.getValue();
            if (num == 1) {
              iter.remove();
            } else {
              entry.setValue(num - 1);
            }
            return localStorage;
          }
        }
      }
    }
    return null;
  }

  /**
   * Choose <i>localMachine</i> as the target.
   * if <i>localMachine</i> is not available,
   * choose a node on the same rack
   * @return the chosen storage
   */
  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes, boolean fallbackToLocalRack)
      throws NotEnoughReplicasException {
    DatanodeStorageInfo localStorage = chooseLocalStorage(localMachine,
        excludedNodes, blocksize, maxNodesPerRack, results,
        avoidStaleNodes, storageTypes);
    if (localStorage != null) {
      return localStorage;
    }

    if (!fallbackToLocalRack) {
      return null;
    }
    // try a node on local rack
    return chooseLocalRack(localMachine, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);
  }
  
  /**
   * Add <i>localMachine</i> and related nodes to <i>excludedNodes</i>
   * for next replica choosing. In sub class, we can add more nodes within
   * the same failure domain of localMachine
   * @return number of new excluded nodes
   */
  protected int addToExcludedNodes(DatanodeDescriptor localMachine,
      Set<Node> excludedNodes) {
    return excludedNodes.add(localMachine) ? 1 : 0;
  }

  /**
   * Choose one node from the rack that <i>localMachine</i> is on.
   * if no such node is available, choose one node from the rack where
   * a second replica is on.
   * if still no such node is available, choose a random node 
   * in the cluster.
   * @return the chosen node
   */
  protected DatanodeStorageInfo chooseLocalRack(Node localMachine,
                                                Set<Node> excludedNodes,
                                                long blocksize,
                                                int maxNodesPerRack,
                                                List<DatanodeStorageInfo> results,
                                                boolean avoidStaleNodes,
                                                EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    final String localRack = localMachine.getNetworkLocation();
      
    try {
      // choose one from the local rack
      return chooseRandom(localRack, excludedNodes,
          blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      // find the next replica and retry with its rack
      for(DatanodeStorageInfo resultStorage : results) {
        DatanodeDescriptor nextNode = resultStorage.getDatanodeDescriptor();
        if (nextNode != localMachine) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failed to choose from local rack (location = {}), retry with the rack "
                + "of the next replica (location = {})", localRack,
                nextNode.getNetworkLocation(), e);
          }
          return chooseFromNextRack(nextNode, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes, storageTypes);
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose from local rack (location = {}); the second"
            + " replica is not found, retry choosing randomly", localRack, e);
      }

      //the second replica is not found, randomly choose one from the network
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  private DatanodeStorageInfo chooseFromNextRack(Node next,
      Set<Node> excludedNodes,
      long blocksize,
      int maxNodesPerRack,
      List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes) throws NotEnoughReplicasException {
    final String nextRack = next.getNetworkLocation();
    try {
      return chooseRandom(nextRack, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      LOG.debug("Failed to choose from the next rack (location = {}), retry choosing randomly",
          nextRack, e);

      // otherwise randomly choose one from the network
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  /** 
   * Choose <i>numOfReplicas</i> nodes from the racks 
   * that <i>localMachine</i> is NOT on.
   * If not enough nodes are available, choose the remaining ones
   * from the local rack
   */
  protected void chooseRemoteRack(int numOfReplicas,
                                DatanodeDescriptor localMachine,
                                Set<Node> excludedNodes,
                                long blocksize,
                                int maxReplicasPerRack,
                                List<DatanodeStorageInfo> results,
                                boolean avoidStaleNodes,
                                EnumMap<StorageType, Integer> storageTypes)
                                    throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // randomly choose one node from remote racks
    try {
      chooseRandom(numOfReplicas, "~" + localMachine.getNetworkLocation(),
          excludedNodes, blocksize, maxReplicasPerRack, results,
          avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      LOG.debug("Failed to choose remote rack (location = ~{}), fallback to local rack",
          localMachine.getNetworkLocation(), e);
      chooseRandom(numOfReplicas-(results.size()-oldNumOfReplicas),
                   localMachine.getNetworkLocation(), excludedNodes, blocksize, 
                   maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  /**
   * Randomly choose one target from the given <i>scope</i>.
   * @return the chosen storage, if there is any.
   */
  protected DatanodeStorageInfo chooseRandom(String scope,
      Set<Node> excludedNodes,
      long blocksize,
      int maxNodesPerRack,
      List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes)
          throws NotEnoughReplicasException {
    return chooseRandom(1, scope, excludedNodes, blocksize, maxNodesPerRack,
        results, avoidStaleNodes, storageTypes);
  }

  /**
   * Randomly choose <i>numOfReplicas</i> targets from the given <i>scope</i>.
   * @return the first chosen node, if there is any.
   */
  protected DatanodeStorageInfo chooseRandom(int numOfReplicas,
                            String scope,
                            Set<Node> excludedNodes,
                            long blocksize,
                            int maxNodesPerRack,
                            List<DatanodeStorageInfo> results,
                            boolean avoidStaleNodes,
                            EnumMap<StorageType, Integer> storageTypes)
                            throws NotEnoughReplicasException {
    StringBuilder builder = debugLoggingBuilder.get();
    if (LOG.isDebugEnabled()) {
      builder.setLength(0);
      builder.append("[");
    }
    CHOOSE_RANDOM_REASONS.get().clear();
    boolean badTarget = false;
    DatanodeStorageInfo firstChosen = null;
    while (numOfReplicas > 0) {
      // the storage type that current node has
      StorageType includeType = null;
      DatanodeDescriptor chosenNode = null;
      if (clusterMap instanceof DFSNetworkTopology) {
        for (StorageType type : storageTypes.keySet()) {
          chosenNode = chooseDataNode(scope, excludedNodes, type);

          if (chosenNode != null) {
            includeType = type;
            break;
          }
          logNodeIsNotChosen(null,
              NodeNotChosenReason.NO_REQUIRED_STORAGE_TYPE,
              " for storage type " + type);
        }
      } else {
        chosenNode = chooseDataNode(scope, excludedNodes);
      }

      if (chosenNode == null) {
        break;
      }
      Preconditions.checkState(excludedNodes.add(chosenNode), "chosenNode "
          + chosenNode + " is already in excludedNodes " + excludedNodes);
      if (LOG.isDebugEnabled()) {
        builder.append("\nNode ").append(NodeBase.getPath(chosenNode))
            .append(" [");
      }
      DatanodeStorageInfo storage = null;
      if (isGoodDatanode(chosenNode, maxNodesPerRack, considerLoad,
          results, avoidStaleNodes)) {
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
            .entrySet().iterator(); iter.hasNext();) {
          Map.Entry<StorageType, Integer> entry = iter.next();

          // If there is one storage type the node has already contained,
          // then no need to loop through other storage type.
          if (includeType != null && entry.getKey() != includeType) {
            continue;
          }

          storage = chooseStorage4Block(
              chosenNode, blocksize, results, entry.getKey());
          if (storage != null) {
            numOfReplicas--;
            if (firstChosen == null) {
              firstChosen = storage;
            }
            // add node (subclasses may also add related nodes) to excludedNode
            addToExcludedNodes(chosenNode, excludedNodes);
            int num = entry.getValue();
            if (num == 1) {
              iter.remove();
            } else {
              entry.setValue(num - 1);
            }
            break;
          }
        }

        if (LOG.isDebugEnabled()) {
          builder.append("\n]");
        }

        // If no candidate storage was found on this DN then set badTarget.
        badTarget = (storage == null);
      }
    }
    if (numOfReplicas>0) {
      String detail = enableDebugLogging;
      if (LOG.isDebugEnabled()) {
        detail = builder.toString();
        if (badTarget) {
          builder.setLength(0);
        } else {
          if (detail.length() > 1) {
            // only log if there's more than "[", which is always appended at
            // the beginning of this method.
            LOG.debug(detail);
          }
          detail = "";
        }
      }
      final HashMap<NodeNotChosenReason, Integer> reasonMap =
          CHOOSE_RANDOM_REASONS.get();
      if (!reasonMap.isEmpty()) {
        LOG.info("Not enough replicas was chosen. Reason: {}", reasonMap);
      }
      throw new NotEnoughReplicasException(detail);
    }
    
    return firstChosen;
  }

  /**
   * Choose a datanode from the given <i>scope</i>.
   * @return the chosen node, if there is any.
   */
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNodes) {
    return (DatanodeDescriptor) clusterMap.chooseRandom(scope, excludedNodes);
  }

  /**
   * Choose a datanode from the given <i>scope</i> with specified
   * storage type.
   * @return the chosen node, if there is any.
   */
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNodes, StorageType type) {
    return (DatanodeDescriptor) ((DFSNetworkTopology) clusterMap)
        .chooseRandomWithStorageTypeTwoTrial(scope, excludedNodes, type);
  }

  /**
   * Choose a good storage of given storage type from datanode, and add it to
   * the result list.
   *
   * @param dnd datanode descriptor
   * @param blockSize requested block size
   * @param results the result storages
   * @param storageType requested storage type
   * @return the chosen datanode storage
   */
  DatanodeStorageInfo chooseStorage4Block(DatanodeDescriptor dnd,
      long blockSize,
      List<DatanodeStorageInfo> results,
      StorageType storageType) {
    DatanodeStorageInfo storage =
        dnd.chooseStorage4Block(storageType, blockSize, minBlocksForWrite);
    if (storage != null) {
      results.add(storage);
    } else {
      logNodeIsNotChosen(dnd, NodeNotChosenReason.NOT_ENOUGH_STORAGE_SPACE,
          " for storage type " + storageType);
    }
    return storage;
  }

  private static void logNodeIsNotChosen(DatanodeDescriptor node,
      NodeNotChosenReason reason) {
    logNodeIsNotChosen(node, reason, null);
  }

  private static void logNodeIsNotChosen(DatanodeDescriptor node,
      NodeNotChosenReason reason, String reasonDetails) {
    assert reason != null;
    if (LOG.isDebugEnabled()) {
      // build the error message for later use.
      debugLoggingBuilder.get()
          .append("\n  Datanode ").append((node==null)?"None":node)
          .append(" is not chosen since ").append(reason.getText());
      if (reasonDetails != null) {
        debugLoggingBuilder.get().append(" ").append(reasonDetails);
      }
      debugLoggingBuilder.get().append(".");
    }
    // always populate reason map to log high level reasons.
    final HashMap<NodeNotChosenReason, Integer> reasonMap =
        CHOOSE_RANDOM_REASONS.get();
    Integer base = reasonMap.get(reason);
    if (base == null) {
      base = 0;
    }
    reasonMap.put(reason, base + 1);
  }

  /**
   * Determine if a datanode should be chosen based on current workload.
   *
   * @param node The target datanode
   * @return Return true if the datanode should be excluded, otherwise false
   */
  boolean excludeNodeByLoad(DatanodeDescriptor node){
    double inServiceXceiverCount = getInServiceXceiverAverage(node);
    final double maxLoad = considerLoadFactor * inServiceXceiverCount;

    final int nodeLoad = node.getXceiverCount();
    if ((nodeLoad > maxLoad) && (maxLoad > 0)) {
      logNodeIsNotChosen(node, NodeNotChosenReason.NODE_TOO_BUSY,
          "(load: " + nodeLoad + " > " + maxLoad + ")");
      return true;
    }
    if (considerLoadByVolume) {
      final int numVolumesAvailable = node.getNumVolumesAvailable();
      final double maxLoadForVolumes = considerLoadFactor * numVolumesAvailable *
          stats.getInServiceXceiverAverageForVolume();
      if (maxLoadForVolumes > 0.0 && nodeLoad > maxLoadForVolumes) {
        logNodeIsNotChosen(node, NodeNotChosenReason.NODE_TOO_BUSY_BY_VOLUME,
            "(load: " + nodeLoad + " > " + maxLoadForVolumes + ") ");
        return true;
      }
    }
    return false;
  }

  /**
   * Gets the inServiceXceiver average count for the cluster, if
   * considerLoadByStorageType is true, then load is calculated only for the
   * storage types present on the datanode.
   * @param node the datanode whose storage types are to be taken into account.
   * @return the InServiceXceiverAverage count.
   */
  private double getInServiceXceiverAverage(DatanodeDescriptor node) {
    double inServiceXceiverCount;
    if (considerLoadByStorageType) {
      inServiceXceiverCount =
          getInServiceXceiverAverageByStorageType(node.getStorageTypes());
    } else {
      inServiceXceiverCount = stats.getInServiceXceiverAverage();
    }
    return inServiceXceiverCount;
  }

  /**
   * Gets the average xceiver count with respect to the storage types.
   * @param storageTypes the storage types.
   * @return the average xceiver count wrt the provided storage types.
   */
  private double getInServiceXceiverAverageByStorageType(
      Set<StorageType> storageTypes) {
    double avgLoad = 0;
    final Map<StorageType, StorageTypeStats> storageStats =
        stats.getStorageTypeStats();
    int numNodes = 0;
    int numXceiver = 0;
    for (StorageType s : storageTypes) {
      StorageTypeStats storageTypeStats = storageStats.get(s);
      numNodes += storageTypeStats.getNodesInService();
      numXceiver += storageTypeStats.getNodesInServiceXceiverCount();
    }
    if (numNodes != 0) {
      avgLoad = (double) numXceiver / numNodes;
    }

    return avgLoad;
  }

  /**
   * Determine if a datanode is good for placing block.
   *
   * @param node The target datanode
   * @param maxTargetPerRack Maximum number of targets per rack. The value of
   *                       this parameter depends on the number of racks in
   *                       the cluster and total number of replicas for a block
   * @param considerLoad whether or not to consider load of the target node
   * @param results A list containing currently chosen nodes. Used to check if
   *                too many nodes has been chosen in the target rack.
   * @param avoidStaleNodes Whether or not to avoid choosing stale nodes
   * @return Return true if the datanode is good candidate, otherwise false
   */
  boolean isGoodDatanode(DatanodeDescriptor node,
                         int maxTargetPerRack, boolean considerLoad,
                         List<DatanodeStorageInfo> results,
                         boolean avoidStaleNodes) {
    // check if the node is in state 'In Service'
    if (!node.isInService()) {
      logNodeIsNotChosen(node, NodeNotChosenReason.NOT_IN_SERVICE);
      return false;
    }

    // check if the target is a stale node
    if (avoidStaleNodes) {
      if (node.isStale(this.staleInterval)) {
        logNodeIsNotChosen(node, NodeNotChosenReason.NODE_STALE);
        return false;
      }
    }

    // check the communication traffic of the target machine
    if(considerLoad){
      if(excludeNodeByLoad(node)){
        return false;
      }
    }
      
    // check if the target rack has chosen too many nodes
    String rackname = node.getNetworkLocation();
    int counter=1;
    for(DatanodeStorageInfo resultStorage : results) {
      if (rackname.equals(
          resultStorage.getDatanodeDescriptor().getNetworkLocation())) {
        counter++;
      }
    }
    if (counter > maxTargetPerRack) {
      logNodeIsNotChosen(node, NodeNotChosenReason.TOO_MANY_NODES_ON_RACK);
      return false;
    }

    // check if the target is a slow node
    if (dataNodePeerStatsEnabled && excludeSlowNodesEnabled) {
      Set<String> slowNodesUuidSet = DatanodeManager.getSlowNodesUuidSet();
      if (slowNodesUuidSet.contains(node.getDatanodeUuid())) {
        logNodeIsNotChosen(node, NodeNotChosenReason.NODE_SLOW);
        return false;
      }
    }

    return true;
  }

  /**
   * Return a pipeline of nodes.
   * The pipeline is formed finding a shortest path that 
   * starts from the writer and traverses all <i>nodes</i>
   * This is basically a traveling salesman problem.
   */
  private DatanodeStorageInfo[] getPipeline(Node writer,
      DatanodeStorageInfo[] storages) {
    if (storages.length == 0) {
      return storages;
    }

    synchronized(clusterMap) {
      int index=0;
      if (writer == null || !clusterMap.contains(writer)) {
        writer = storages[0].getDatanodeDescriptor();
      }
      for(; index < storages.length; index++) {
        DatanodeStorageInfo shortestStorage = storages[index];
        int shortestDistance = clusterMap.getDistance(writer,
            shortestStorage.getDatanodeDescriptor());
        int shortestIndex = index;
        for(int i = index + 1; i < storages.length; i++) {
          int currentDistance = clusterMap.getDistance(writer,
              storages[i].getDatanodeDescriptor());
          if (shortestDistance>currentDistance) {
            shortestDistance = currentDistance;
            shortestStorage = storages[i];
            shortestIndex = i;
          }
        }
        //switch position index & shortestIndex
        if (index != shortestIndex) {
          storages[shortestIndex] = storages[index];
          storages[index] = shortestStorage;
        }
        writer = shortestStorage.getDatanodeDescriptor();
      }
    }
    return storages;
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas) {
    if (locs == null) {
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    }
    if (!clusterMap.hasClusterEverBeenMultiRack()) {
      // only one rack
      return ONE_RACK_PLACEMENT;
    }
    final int minRacks = Math.min(2, numberOfReplicas);
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    final long rackCount = Arrays.asList(locs).stream()
        .map(dn -> dn.getNetworkLocation()).distinct().count();

    return new BlockPlacementStatusDefault(Math.toIntExact(rackCount),
        minRacks, clusterMap.getNumOfNonEmptyRacks());
  }

  /**
   * Decide whether deleting the specified replica of the block still makes
   * the block conform to the configured block placement policy.
   * @param moreThanOne The replica locations of this block that are present
   *                    on more than one unique racks.
   * @param exactlyOne Replica locations of this block that  are present
   *                    on exactly one unique racks.
   * @param excessTypes The excess {@link StorageType}s according to the
   *                    {@link BlockStoragePolicy}.
   *
   * @return the replica that is the best candidate for deletion
   */
  @VisibleForTesting
  public DatanodeStorageInfo chooseReplicaToDelete(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      final List<StorageType> excessTypes,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    long oldestHeartbeat =
      monotonicNow() - heartbeatInterval * tolerateHeartbeatMultiplier;
    DatanodeStorageInfo oldestHeartbeatStorage = null;
    long minSpace = Long.MAX_VALUE;
    DatanodeStorageInfo minSpaceStorage = null;

    // Pick the node with the oldest heartbeat or with the least free space,
    // if all heartbeats are within the tolerable heartbeat interval
    for(DatanodeStorageInfo storage : pickupReplicaSet(moreThanOne,
        exactlyOne, rackMap)) {
      if (!excessTypes.contains(storage.getStorageType())) {
        continue;
      }

      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      long free = storage.getRemaining();
      long lastHeartbeat = node.getLastUpdateMonotonic();
      if (lastHeartbeat < oldestHeartbeat) {
        oldestHeartbeat = lastHeartbeat;
        oldestHeartbeatStorage = storage;
      }
      if (minSpace > free) {
        minSpace = free;
        minSpaceStorage = storage;
      }
    }

    final DatanodeStorageInfo storage;
    if (oldestHeartbeatStorage != null) {
      storage = oldestHeartbeatStorage;
    } else if (minSpaceStorage != null) {
      storage = minSpaceStorage;
    } else {
      return null;
    }
    excessTypes.remove(storage.getStorageType());
    return storage;
  }

  @Override
  public List<DatanodeStorageInfo> chooseReplicasToDelete(
      Collection<DatanodeStorageInfo> availableReplicas,
      Collection<DatanodeStorageInfo> delCandidates,
      int expectedNumOfReplicas,
      List<StorageType> excessTypes,
      DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {

    List<DatanodeStorageInfo> excessReplicas = new ArrayList<>();

    final Map<String, List<DatanodeStorageInfo>> rackMap = new HashMap<>();

    final List<DatanodeStorageInfo> moreThanOne = new ArrayList<>();
    final List<DatanodeStorageInfo> exactlyOne = new ArrayList<>();

    // split candidate nodes for deletion into two sets
    // moreThanOne contains nodes on rack with more than one replica
    // exactlyOne contains the remaining nodes
    splitNodesWithRack(availableReplicas, delCandidates, rackMap, moreThanOne,
        exactlyOne);

    // pick one node to delete that favors the delete hint
    // otherwise pick one with least space from priSet if it is not empty
    // otherwise one node with least space from remains
    boolean firstOne = true;
    final DatanodeStorageInfo delNodeHintStorage =
        DatanodeStorageInfo.getDatanodeStorageInfo(delCandidates, delNodeHint);
    final DatanodeStorageInfo addedNodeStorage =
        DatanodeStorageInfo.getDatanodeStorageInfo(delCandidates, addedNode);

    while (delCandidates.size() - expectedNumOfReplicas > excessReplicas.size()) {
      final DatanodeStorageInfo cur;
      if (firstOne && useDelHint(delNodeHintStorage, addedNodeStorage,
          moreThanOne, exactlyOne, excessTypes)) {
        cur = delNodeHintStorage;
      } else { // regular excessive replica removal
        cur = chooseReplicaToDelete(moreThanOne, exactlyOne,
            excessTypes, rackMap);
      }
      firstOne = false;
      if (cur == null) {
        LOG.debug(
            "No excess replica can be found. excessTypes: {}. moreThanOne: {}. exactlyOne: {}.",
            excessTypes, moreThanOne, exactlyOne);
        break;
      }

      // adjust rackmap, moreThanOne, and exactlyOne
      adjustSetsWithChosenReplica(rackMap, moreThanOne, exactlyOne, cur);
      excessReplicas.add(cur);
    }
    return excessReplicas;
  }

  /** Check if we can use delHint. */
  @VisibleForTesting
  boolean useDelHint(DatanodeStorageInfo delHint,
      DatanodeStorageInfo added, List<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      List<StorageType> excessTypes) {
    if (delHint == null) {
      return false; // no delHint
    } else if (!excessTypes.contains(delHint.getStorageType())) {
      return false; // delHint storage type is not an excess type
    } else {
      // check if removing delHint reduces the number of racks
      return notReduceNumOfGroups(moreThanOne, delHint, added);
    }
  }

  // Check if moving from source to target will reduce the number of
  // groups. The groups could be based on racks or upgrade domains.
  <T> boolean notReduceNumOfGroups(List<T> moreThanOne, T source, T target) {
    if (moreThanOne.contains(source)) {
      return true; // source and some other nodes are under the same group.
    } else if (target != null && !moreThanOne.contains(target)) {
      return true; // the added node adds a new group.
    }
    return false; // removing delHint reduces the number of groups.
  }

  @Override
  public boolean isMovable(Collection<DatanodeInfo> locs,
      DatanodeInfo source, DatanodeInfo target) {
    final Map<String, List<DatanodeInfo>> rackMap = new HashMap<>();
    final List<DatanodeInfo> moreThanOne = new ArrayList<>();
    final List<DatanodeInfo> exactlyOne = new ArrayList<>();
    splitNodesWithRack(locs, locs, rackMap, moreThanOne, exactlyOne);
    return notReduceNumOfGroups(moreThanOne, source, target);
  }

  /**
   * Pick up replica node set for deleting replica as over-replicated. 
   * First set contains replica nodes on rack with more than one
   * replica while second set contains remaining replica nodes.
   * If only 1 rack, pick all. If 2 racks, pick all that have more than
   * 1 replicas on the same rack; if no such replicas, pick all.
   * If 3 or more racks, pick all.
   */
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    Collection<DatanodeStorageInfo> ret = new ArrayList<>();
    if (rackMap.size() == 2) {
      for (List<DatanodeStorageInfo> dsi : rackMap.values()) {
        if (dsi.size() >= 2) {
          ret.addAll(dsi);
        }
      }
    }
    if (ret.isEmpty()) {
      // Return all replicas if rackMap.size() != 2
      // or rackMap.size() == 2 but no shared replicas on any rack
      ret.addAll(moreThanOne);
      ret.addAll(exactlyOne);
    }
    return ret;
  }

  @VisibleForTesting
  void setPreferLocalNode(boolean prefer) {
    this.preferLocalNode = prefer;
  }

  @Override
  public void setExcludeSlowNodesEnabled(boolean enable) {
    this.excludeSlowNodesEnabled = enable;
  }

  @Override
  public boolean getExcludeSlowNodesEnabled() {
    return excludeSlowNodesEnabled;
  }

  @Override
  public void setMinBlocksForWrite(int minBlocksForWrite) {
    this.minBlocksForWrite = minBlocksForWrite;
  }

  @Override
  public int getMinBlocksForWrite() {
    return minBlocksForWrite;
  }
}
