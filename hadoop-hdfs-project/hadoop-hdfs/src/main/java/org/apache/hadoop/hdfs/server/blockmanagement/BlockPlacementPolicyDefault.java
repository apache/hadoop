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

import static org.apache.hadoop.util.Time.now;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import com.google.common.annotations.VisibleForTesting;

/**
 * The class is responsible for choosing the desired number of targets
 * for placing block replicas.
 * The replica placement strategy is that if the writer is on a datanode,
 * the 1st replica is placed on the local machine, 
 * otherwise a random datanode. The 2nd replica is placed on a datanode
 * that is on a different rack. The 3rd replica is placed on a datanode
 * which is on a different node of the rack as the second replica.
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyDefault extends BlockPlacementPolicy {

  private static final String enableDebugLogging =
    "For more information, please enable DEBUG log level on "
    + BlockPlacementPolicy.class.getName();

  private static final ThreadLocal<StringBuilder> debugLoggingBuilder
      = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  protected boolean considerLoad; 
  private boolean preferLocalNode = true;
  protected NetworkTopology clusterMap;
  protected Host2NodesMap host2datanodeMap;
  private FSClusterStats stats;
  protected long heartbeatInterval;   // interval for DataNode heartbeats
  private long staleInterval;   // interval used to identify stale DataNodes
  
  /**
   * A miss of that many heartbeats is tolerated for replica deletion policy.
   */
  protected int tolerateHeartbeatMultiplier;

  protected BlockPlacementPolicyDefault(Configuration conf, FSClusterStats stats,
                           NetworkTopology clusterMap, 
                           Host2NodesMap host2datanodeMap) {
    initialize(conf, stats, clusterMap, host2datanodeMap);
  }

  protected BlockPlacementPolicyDefault() {
  }
    
  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
                         NetworkTopology clusterMap, 
                         Host2NodesMap host2datanodeMap) {
    this.considerLoad = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, true);
    this.stats = stats;
    this.clusterMap = clusterMap;
    this.host2datanodeMap = host2datanodeMap;
    this.heartbeatInterval = conf.getLong(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000;
    this.tolerateHeartbeatMultiplier = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY,
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT);
    this.staleInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    Node writer,
                                    List<DatanodeStorageInfo> chosenNodes,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blocksize,
                                    StorageType storageType) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, returnChosenNodes,
        excludedNodes, blocksize, storageType);
  }

  @Override
  DatanodeStorageInfo[] chooseTarget(String src,
      int numOfReplicas,
      Node writer,
      Set<Node> excludedNodes,
      long blocksize,
      List<DatanodeDescriptor> favoredNodes,
      StorageType storageType) {
    try {
      if (favoredNodes == null || favoredNodes.size() == 0) {
        // Favored nodes not specified, fall back to regular block placement.
        return chooseTarget(src, numOfReplicas, writer,
            new ArrayList<DatanodeStorageInfo>(numOfReplicas), false, 
            excludedNodes, blocksize, storageType);
      }

      Set<Node> favoriteAndExcludedNodes = excludedNodes == null ?
          new HashSet<Node>() : new HashSet<Node>(excludedNodes);

      // Choose favored nodes
      List<DatanodeStorageInfo> results = new ArrayList<DatanodeStorageInfo>();
      boolean avoidStaleNodes = stats != null
          && stats.isAvoidingStaleDataNodesForWrite();
      for (int i = 0; i < favoredNodes.size() && results.size() < numOfReplicas; i++) {
        DatanodeDescriptor favoredNode = favoredNodes.get(i);
        // Choose a single node which is local to favoredNode.
        // 'results' is updated within chooseLocalNode
        final DatanodeStorageInfo target = chooseLocalStorage(favoredNode,
            favoriteAndExcludedNodes, blocksize, 
            getMaxNodesPerRack(results.size(), numOfReplicas)[1],
            results, avoidStaleNodes, storageType, false);
        if (target == null) {
          LOG.warn("Could not find a target for file " + src
              + " with favored node " + favoredNode); 
          continue;
        }
        favoriteAndExcludedNodes.add(target.getDatanodeDescriptor());
      }

      if (results.size() < numOfReplicas) {
        // Not enough favored nodes, choose other nodes.
        numOfReplicas -= results.size();
        DatanodeStorageInfo[] remainingTargets = 
            chooseTarget(src, numOfReplicas, writer, results,
                false, favoriteAndExcludedNodes, blocksize, storageType);
        for (int i = 0; i < remainingTargets.length; i++) {
          results.add(remainingTargets[i]);
        }
      }
      return getPipeline(writer,
          results.toArray(new DatanodeStorageInfo[results.size()]));
    } catch (NotEnoughReplicasException nr) {
      // Fall back to regular block placement disregarding favored nodes hint
      return chooseTarget(src, numOfReplicas, writer, 
          new ArrayList<DatanodeStorageInfo>(numOfReplicas), false, 
          excludedNodes, blocksize, storageType);
    }
  }

  /** This is the implementation. */
  private DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
                                    Node writer,
                                    List<DatanodeStorageInfo> chosenStorage,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blocksize,
                                    StorageType storageType) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return DatanodeStorageInfo.EMPTY_ARRAY;
    }
      
    if (excludedNodes == null) {
      excludedNodes = new HashSet<Node>();
    }
     
    int[] result = getMaxNodesPerRack(chosenStorage.size(), numOfReplicas);
    numOfReplicas = result[0];
    int maxNodesPerRack = result[1];
      
    final List<DatanodeStorageInfo> results = new ArrayList<DatanodeStorageInfo>(chosenStorage);
    for (DatanodeStorageInfo storage : chosenStorage) {
      // add localMachine and related nodes to excludedNodes
      addToExcludedNodes(storage.getDatanodeDescriptor(), excludedNodes);
    }
      
    if (!clusterMap.contains(writer)) {
      writer = null;
    }
      
    boolean avoidStaleNodes = (stats != null
        && stats.isAvoidingStaleDataNodesForWrite());
    Node localNode = chooseTarget(numOfReplicas, writer,
        excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes, storageType);
    if (!returnChosenNodes) {  
      results.removeAll(chosenStorage);
    }
      
    // sorting nodes to form a pipeline
    return getPipeline((writer==null)?localNode:writer,
                       results.toArray(new DatanodeStorageInfo[results.size()]));
  }

  private int[] getMaxNodesPerRack(int numOfChosen, int numOfReplicas) {
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = numOfChosen + numOfReplicas;
    if (totalNumOfReplicas > clusterSize) {
      numOfReplicas -= (totalNumOfReplicas-clusterSize);
      totalNumOfReplicas = clusterSize;
    }
    int maxNodesPerRack = (totalNumOfReplicas-1)/clusterMap.getNumOfRacks()+2;
    return new int[] {numOfReplicas, maxNodesPerRack};
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
   * @return local node of writer (not chosen node)
   */
  private Node chooseTarget(int numOfReplicas,
                                          Node writer,
                                          Set<Node> excludedNodes,
                                          long blocksize,
                                          int maxNodesPerRack,
                                          List<DatanodeStorageInfo> results,
                                          final boolean avoidStaleNodes,
                                          StorageType storageType) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return writer;
    }
    int totalReplicasExpected = numOfReplicas + results.size();
      
    int numOfResults = results.size();
    boolean newBlock = (numOfResults==0);
    if ((writer == null || !(writer instanceof DatanodeDescriptor)) && !newBlock) {
      writer = results.get(0).getDatanodeDescriptor();
    }

    // Keep a copy of original excludedNodes
    final Set<Node> oldExcludedNodes = avoidStaleNodes ? 
        new HashSet<Node>(excludedNodes) : null;
    try {
      if (numOfResults == 0) {
        writer = chooseLocalStorage(writer, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes, storageType, true)
                .getDatanodeDescriptor();
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      final DatanodeDescriptor dn0 = results.get(0).getDatanodeDescriptor();
      if (numOfResults <= 1) {
        chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageType);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      if (numOfResults <= 2) {
        final DatanodeDescriptor dn1 = results.get(1).getDatanodeDescriptor();
        if (clusterMap.isOnSameRack(dn0, dn1)) {
          chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
              results, avoidStaleNodes, storageType);
        } else if (newBlock){
          chooseLocalRack(dn1, excludedNodes, blocksize, maxNodesPerRack,
              results, avoidStaleNodes, storageType);
        } else {
          chooseLocalRack(writer, excludedNodes, blocksize, maxNodesPerRack,
              results, avoidStaleNodes, storageType);
        }
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageType);
    } catch (NotEnoughReplicasException e) {
      final String message = "Failed to place enough replicas, still in need of "
          + (totalReplicasExpected - results.size()) + " to reach "
          + totalReplicasExpected + ".";
      if (LOG.isTraceEnabled()) {
        LOG.trace(message, e);
      } else {
        LOG.warn(message + " " + e.getMessage());
      }

      if (avoidStaleNodes) {
        // Retry chooseTarget again, this time not avoiding stale nodes.

        // excludedNodes contains the initial excludedNodes and nodes that were
        // not chosen because they were stale, decommissioned, etc.
        // We need to additionally exclude the nodes that were added to the 
        // result list in the successful calls to choose*() above.
        for (DatanodeStorageInfo resultStorage : results) {
          addToExcludedNodes(resultStorage.getDatanodeDescriptor(), oldExcludedNodes);
        }
        // Set numOfReplicas, since it can get out of sync with the result list
        // if the NotEnoughReplicasException was thrown in chooseRandom().
        numOfReplicas = totalReplicasExpected - results.size();
        return chooseTarget(numOfReplicas, writer, oldExcludedNodes, blocksize,
            maxNodesPerRack, results, false, storageType);
      }
    }
    return writer;
  }
    
  /**
   * Choose <i>localMachine</i> as the target.
   * if <i>localMachine</i> is not available, 
   * choose a node on the same rack
   * @return the chosen storage
   */
  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
                                             Set<Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeStorageInfo> results,
                                             boolean avoidStaleNodes,
                                             StorageType storageType,
                                             boolean fallbackToLocalRack)
      throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageType);
    }
    if (preferLocalNode && localMachine instanceof DatanodeDescriptor) {
      DatanodeDescriptor localDatanode = (DatanodeDescriptor) localMachine;
      // otherwise try local machine first
      if (excludedNodes.add(localMachine)) { // was not in the excluded list
        for(DatanodeStorageInfo localStorage : DFSUtil.shuffle(
            localDatanode.getStorageInfos())) {
          if (addIfIsGoodTarget(localStorage, excludedNodes, blocksize,
              maxNodesPerRack, false, results, avoidStaleNodes, storageType) >= 0) {
            return localStorage;
          }
        }
      } 
    }

    if (!fallbackToLocalRack) {
      return null;
    }
    // try a node on local rack
    return chooseLocalRack(localMachine, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageType);
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
                                             StorageType storageType)
      throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageType);
    }
      
    // choose one from the local rack
    try {
      return chooseRandom(localMachine.getNetworkLocation(), excludedNodes,
          blocksize, maxNodesPerRack, results, avoidStaleNodes, storageType);
    } catch (NotEnoughReplicasException e1) {
      // find the second replica
      DatanodeDescriptor newLocal=null;
      for(DatanodeStorageInfo resultStorage : results) {
        DatanodeDescriptor nextNode = resultStorage.getDatanodeDescriptor();
        if (nextNode != localMachine) {
          newLocal = nextNode;
          break;
        }
      }
      if (newLocal != null) {
        try {
          return chooseRandom(newLocal.getNetworkLocation(), excludedNodes,
              blocksize, maxNodesPerRack, results, avoidStaleNodes, storageType);
        } catch(NotEnoughReplicasException e2) {
          //otherwise randomly choose one from the network
          return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes, storageType);
        }
      } else {
        //otherwise randomly choose one from the network
        return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes, storageType);
      }
    }
  }
    
  /** 
   * Choose <i>numOfReplicas</i> nodes from the racks 
   * that <i>localMachine</i> is NOT on.
   * if not enough nodes are available, choose the remaining ones 
   * from the local rack
   */
    
  protected void chooseRemoteRack(int numOfReplicas,
                                DatanodeDescriptor localMachine,
                                Set<Node> excludedNodes,
                                long blocksize,
                                int maxReplicasPerRack,
                                List<DatanodeStorageInfo> results,
                                boolean avoidStaleNodes,
                                StorageType storageType)
                                    throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // randomly choose one node from remote racks
    try {
      chooseRandom(numOfReplicas, "~" + localMachine.getNetworkLocation(),
          excludedNodes, blocksize, maxReplicasPerRack, results,
          avoidStaleNodes, storageType);
    } catch (NotEnoughReplicasException e) {
      chooseRandom(numOfReplicas-(results.size()-oldNumOfReplicas),
                   localMachine.getNetworkLocation(), excludedNodes, blocksize, 
                   maxReplicasPerRack, results, avoidStaleNodes, storageType);
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
      StorageType storageType)
          throws NotEnoughReplicasException {
    return chooseRandom(1, scope, excludedNodes, blocksize, maxNodesPerRack,
        results, avoidStaleNodes, storageType);
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
                            StorageType storageType)
                                throws NotEnoughReplicasException {
      
    int numOfAvailableNodes = clusterMap.countNumOfAvailableNodes(
        scope, excludedNodes);
    StringBuilder builder = null;
    if (LOG.isDebugEnabled()) {
      builder = debugLoggingBuilder.get();
      builder.setLength(0);
      builder.append("[");
    }
    boolean badTarget = false;
    DatanodeStorageInfo firstChosen = null;
    while(numOfReplicas > 0 && numOfAvailableNodes > 0) {
      DatanodeDescriptor chosenNode = 
          (DatanodeDescriptor)clusterMap.chooseRandom(scope);
      if (excludedNodes.add(chosenNode)) { //was not in the excluded list
        numOfAvailableNodes--;

        final DatanodeStorageInfo[] storages = DFSUtil.shuffle(
            chosenNode.getStorageInfos());
        int i;
        for(i = 0; i < storages.length; i++) {
          final int newExcludedNodes = addIfIsGoodTarget(storages[i],
              excludedNodes, blocksize, maxNodesPerRack, considerLoad, results,
              avoidStaleNodes, storageType);
          if (newExcludedNodes >= 0) {
            numOfReplicas--;
            if (firstChosen == null) {
              firstChosen = storages[i];
            }
            numOfAvailableNodes -= newExcludedNodes;
            break;
          }
        }

        // If no candidate storage was found on this DN then set badTarget.
        badTarget = (i == storages.length);
      }
    }
      
    if (numOfReplicas>0) {
      String detail = enableDebugLogging;
      if (LOG.isDebugEnabled()) {
        if (badTarget && builder != null) {
          detail = builder.append("]").toString();
          builder.setLength(0);
        } else detail = "";
      }
      throw new NotEnoughReplicasException(detail);
    }
    
    return firstChosen;
  }

  /**
   * If the given storage is a good target, add it to the result list and
   * update the set of excluded nodes.
   * @return -1 if the given is not a good target;
   *         otherwise, return the number of nodes added to excludedNodes set.
   */
  int addIfIsGoodTarget(DatanodeStorageInfo storage,
      Set<Node> excludedNodes,
      long blockSize,
      int maxNodesPerRack,
      boolean considerLoad,
      List<DatanodeStorageInfo> results,                           
      boolean avoidStaleNodes,
      StorageType storageType) {
    if (isGoodTarget(storage, blockSize, maxNodesPerRack, considerLoad,
        results, avoidStaleNodes, storageType)) {
      results.add(storage);
      // add node and related nodes to excludedNode
      return addToExcludedNodes(storage.getDatanodeDescriptor(), excludedNodes);
    } else { 
      return -1;
    }
  }

  private static void logNodeIsNotChosen(DatanodeStorageInfo storage, String reason) {
    if (LOG.isDebugEnabled()) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      // build the error message for later use.
      debugLoggingBuilder.get()
          .append(node).append(": ")
          .append("Storage ").append(storage)
          .append("at node ").append(NodeBase.getPath(node))
          .append(" is not chosen because ")
          .append(reason);
    }
  }

  /**
   * Determine if a storage is a good target. 
   * 
   * @param storage The target storage
   * @param blockSize Size of block
   * @param maxTargetPerRack Maximum number of targets per rack. The value of 
   *                       this parameter depends on the number of racks in 
   *                       the cluster and total number of replicas for a block
   * @param considerLoad whether or not to consider load of the target node
   * @param results A list containing currently chosen nodes. Used to check if 
   *                too many nodes has been chosen in the target rack.
   * @param avoidStaleNodes Whether or not to avoid choosing stale nodes
   * @return Return true if <i>node</i> has enough space, 
   *         does not have too much load, 
   *         and the rack does not have too many nodes.
   */
  private boolean isGoodTarget(DatanodeStorageInfo storage,
                               long blockSize, int maxTargetPerRack,
                               boolean considerLoad,
                               List<DatanodeStorageInfo> results,
                               boolean avoidStaleNodes,
                               StorageType storageType) {
    if (storage.getStorageType() != storageType) {
      logNodeIsNotChosen(storage,
          "storage types do not match, where the expected storage type is "
              + storageType);
      return false;
    }
    if (storage.getState() == State.READ_ONLY_SHARED) {
      logNodeIsNotChosen(storage, "storage is read-only");
      return false;
    }
    DatanodeDescriptor node = storage.getDatanodeDescriptor();
    // check if the node is (being) decommissioned
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      logNodeIsNotChosen(storage, "the node is (being) decommissioned ");
      return false;
    }

    if (avoidStaleNodes) {
      if (node.isStale(this.staleInterval)) {
        logNodeIsNotChosen(storage, "the node is stale ");
        return false;
      }
    }
    
    final long requiredSize = blockSize * HdfsConstants.MIN_BLOCKS_FOR_WRITE;
    final long scheduledSize = blockSize * node.getBlocksScheduled();
    if (requiredSize > storage.getRemaining() - scheduledSize) {
      logNodeIsNotChosen(storage, "the node does not have enough space ");
      return false;
    }

    // check the communication traffic of the target machine
    if (considerLoad) {
      final double maxLoad = 2.0 * stats.getInServiceXceiverAverage();
      final int nodeLoad = node.getXceiverCount();
      if (nodeLoad > maxLoad) {
        logNodeIsNotChosen(storage,
            "the node is too busy (load:"+nodeLoad+" > "+maxLoad+") ");
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
    if (counter>maxTargetPerRack) {
      logNodeIsNotChosen(storage, "the rack has too many chosen nodes ");
      return false;
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
  public BlockPlacementStatus verifyBlockPlacement(String srcPath,
      LocatedBlock lBlk, int numberOfReplicas) {
    DatanodeInfo[] locs = lBlk.getLocations();
    if (locs == null)
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    int numRacks = clusterMap.getNumOfRacks();
    if(numRacks <= 1) // only one rack
      return new BlockPlacementStatusDefault(
          Math.min(numRacks, numberOfReplicas), numRacks);
    int minRacks = Math.min(2, numberOfReplicas);
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    Set<String> racks = new TreeSet<String>();
    for (DatanodeInfo dn : locs)
      racks.add(dn.getNetworkLocation());
    return new BlockPlacementStatusDefault(racks.size(), minRacks);
  }

  @Override
  public DatanodeStorageInfo chooseReplicaToDelete(BlockCollection bc,
      Block block, short replicationFactor,
      Collection<DatanodeStorageInfo> first,
      Collection<DatanodeStorageInfo> second) {
    long oldestHeartbeat =
      now() - heartbeatInterval * tolerateHeartbeatMultiplier;
    DatanodeStorageInfo oldestHeartbeatStorage = null;
    long minSpace = Long.MAX_VALUE;
    DatanodeStorageInfo minSpaceStorage = null;

    // Pick the node with the oldest heartbeat or with the least free space,
    // if all hearbeats are within the tolerable heartbeat interval
    for(DatanodeStorageInfo storage : pickupReplicaSet(first, second)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      long free = node.getRemaining();
      long lastHeartbeat = node.getLastUpdate();
      if(lastHeartbeat < oldestHeartbeat) {
        oldestHeartbeat = lastHeartbeat;
        oldestHeartbeatStorage = storage;
      }
      if (minSpace > free) {
        minSpace = free;
        minSpaceStorage = storage;
      }
    }

    return oldestHeartbeatStorage != null? oldestHeartbeatStorage
        : minSpaceStorage;
  }

  /**
   * Pick up replica node set for deleting replica as over-replicated. 
   * First set contains replica nodes on rack with more than one
   * replica while second set contains remaining replica nodes.
   * So pick up first set if not empty. If first is empty, then pick second.
   */
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> first,
      Collection<DatanodeStorageInfo> second) {
    return first.isEmpty() ? second : first;
  }
  
  @VisibleForTesting
  void setPreferLocalNode(boolean prefer) {
    this.preferLocalNode = prefer;
  }
}

