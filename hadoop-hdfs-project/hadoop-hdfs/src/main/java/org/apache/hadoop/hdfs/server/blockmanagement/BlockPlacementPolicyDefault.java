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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
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
  private FSClusterStats stats;
  protected long heartbeatInterval;   // interval for DataNode heartbeats
  private long staleInterval;   // interval used to identify stale DataNodes
  
  /**
   * A miss of that many heartbeats is tolerated for replica deletion policy.
   */
  protected int tolerateHeartbeatMultiplier;

  BlockPlacementPolicyDefault(Configuration conf,  FSClusterStats stats,
                           NetworkTopology clusterMap) {
    initialize(conf, stats, clusterMap);
  }

  protected BlockPlacementPolicyDefault() {
  }
    
  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
                         NetworkTopology clusterMap) {
    this.considerLoad = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, true);
    this.stats = stats;
    this.clusterMap = clusterMap;
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
  public DatanodeDescriptor[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    Node writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blocksize) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, returnChosenNodes,
        excludedNodes, blocksize);
  }

  @Override
  DatanodeDescriptor[] chooseTarget(String src,
      int numOfReplicas,
      Node writer,
      Set<Node> excludedNodes,
      long blocksize,
      List<DatanodeDescriptor> favoredNodes) {
    try {
      if (favoredNodes == null || favoredNodes.size() == 0) {
        // Favored nodes not specified, fall back to regular block placement.
        return chooseTarget(src, numOfReplicas, writer,
            new ArrayList<DatanodeDescriptor>(numOfReplicas), false, 
            excludedNodes, blocksize);
      }

      Set<Node> favoriteAndExcludedNodes = excludedNodes == null ?
          new HashSet<Node>() : new HashSet<Node>(excludedNodes);

      // Choose favored nodes
      List<DatanodeDescriptor> results = new ArrayList<DatanodeDescriptor>();
      boolean avoidStaleNodes = stats != null
          && stats.isAvoidingStaleDataNodesForWrite();
      for (int i = 0; i < Math.min(favoredNodes.size(), numOfReplicas); i++) {
        DatanodeDescriptor favoredNode = favoredNodes.get(i);
        // Choose a single node which is local to favoredNode.
        // 'results' is updated within chooseLocalNode
        DatanodeDescriptor target = chooseLocalNode(favoredNode,
            favoriteAndExcludedNodes, blocksize, 
            getMaxNodesPerRack(results, 
                numOfReplicas)[1], results, avoidStaleNodes);
        if (target == null) {
          LOG.warn("Could not find a target for file " + src
              + " with favored node " + favoredNode); 
          continue;
        }
        favoriteAndExcludedNodes.add(target);
      }

      if (results.size() < numOfReplicas) {
        // Not enough favored nodes, choose other nodes.
        numOfReplicas -= results.size();
        DatanodeDescriptor[] remainingTargets = 
            chooseTarget(src, numOfReplicas, writer, results,
                false, favoriteAndExcludedNodes, blocksize);
        for (int i = 0; i < remainingTargets.length; i++) {
          results.add(remainingTargets[i]);
        }
      }
      return getPipeline(writer,
          results.toArray(new DatanodeDescriptor[results.size()]));
    } catch (NotEnoughReplicasException nr) {
      // Fall back to regular block placement disregarding favored nodes hint
      return chooseTarget(src, numOfReplicas, writer, 
          new ArrayList<DatanodeDescriptor>(numOfReplicas), false, 
          excludedNodes, blocksize);
    }
  }

  /** This is the implementation. */
  private DatanodeDescriptor[] chooseTarget(int numOfReplicas,
                                    Node writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blocksize) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return DatanodeDescriptor.EMPTY_ARRAY;
    }
      
    if (excludedNodes == null) {
      excludedNodes = new HashSet<Node>();
    }
     
    int[] result = getMaxNodesPerRack(chosenNodes, numOfReplicas);
    numOfReplicas = result[0];
    int maxNodesPerRack = result[1];
      
    List<DatanodeDescriptor> results = 
      new ArrayList<DatanodeDescriptor>(chosenNodes);
    for (DatanodeDescriptor node:chosenNodes) {
      // add localMachine and related nodes to excludedNodes
      addToExcludedNodes(node, excludedNodes);
    }
      
    if (!clusterMap.contains(writer)) {
      writer = null;
    }
      
    boolean avoidStaleNodes = (stats != null
        && stats.isAvoidingStaleDataNodesForWrite());
    Node localNode = chooseTarget(numOfReplicas, writer,
        excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes);
    if (!returnChosenNodes) {  
      results.removeAll(chosenNodes);
    }
      
    // sorting nodes to form a pipeline
    return getPipeline((writer==null)?localNode:writer,
                       results.toArray(new DatanodeDescriptor[results.size()]));
  }

  private int[] getMaxNodesPerRack(List<DatanodeDescriptor> chosenNodes,
      int numOfReplicas) {
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = chosenNodes.size()+numOfReplicas;
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
                                          List<DatanodeDescriptor> results,
                                          final boolean avoidStaleNodes) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return writer;
    }
    int totalReplicasExpected = numOfReplicas + results.size();
      
    int numOfResults = results.size();
    boolean newBlock = (numOfResults==0);
    if ((writer == null || !(writer instanceof DatanodeDescriptor)) && !newBlock) {
      writer = results.get(0);
    }

    // Keep a copy of original excludedNodes
    final Set<Node> oldExcludedNodes = avoidStaleNodes ? 
        new HashSet<Node>(excludedNodes) : null;
    try {
      if (numOfResults == 0) {
        writer = chooseLocalNode(writer, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      if (numOfResults <= 1) {
        chooseRemoteRack(1, results.get(0), excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      if (numOfResults <= 2) {
        if (clusterMap.isOnSameRack(results.get(0), results.get(1))) {
          chooseRemoteRack(1, results.get(0), excludedNodes,
                           blocksize, maxNodesPerRack, 
                           results, avoidStaleNodes);
        } else if (newBlock){
          chooseLocalRack(results.get(1), excludedNodes, blocksize, 
                          maxNodesPerRack, results, avoidStaleNodes);
        } else {
          chooseLocalRack(writer, excludedNodes, blocksize, maxNodesPerRack,
              results, avoidStaleNodes);
        }
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes);
    } catch (NotEnoughReplicasException e) {
      LOG.warn("Not able to place enough replicas, still in need of "
               + (totalReplicasExpected - results.size()) + " to reach "
               + totalReplicasExpected + "\n"
               + e.getMessage());
      if (avoidStaleNodes) {
        // Retry chooseTarget again, this time not avoiding stale nodes.

        // excludedNodes contains the initial excludedNodes and nodes that were
        // not chosen because they were stale, decommissioned, etc.
        // We need to additionally exclude the nodes that were added to the 
        // result list in the successful calls to choose*() above.
        for (Node node : results) {
          oldExcludedNodes.add(node);
        }
        // Set numOfReplicas, since it can get out of sync with the result list
        // if the NotEnoughReplicasException was thrown in chooseRandom().
        numOfReplicas = totalReplicasExpected - results.size();
        return chooseTarget(numOfReplicas, writer, oldExcludedNodes, blocksize,
            maxNodesPerRack, results, false);
      }
    }
    return writer;
  }
    
  /**
   * Choose <i>localMachine</i> as the target.
   * if <i>localMachine</i> is not available, 
   * choose a node on the same rack
   * @return the chosen node
   */
  protected DatanodeDescriptor chooseLocalNode(Node localMachine,
                                             Set<Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeDescriptor> results,
                                             boolean avoidStaleNodes)
      throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
    if (localMachine == null)
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes);
    if (preferLocalNode && localMachine instanceof DatanodeDescriptor) {
      DatanodeDescriptor localDatanode = (DatanodeDescriptor) localMachine;
      // otherwise try local machine first
      if (excludedNodes.add(localMachine)) { // was not in the excluded list
        if (addIfIsGoodTarget(localDatanode, excludedNodes, blocksize,
            maxNodesPerRack, false, results, avoidStaleNodes) >= 0) {
          return localDatanode;
        }
      } 
    }      
    // try a node on local rack
    return chooseLocalRack(localMachine, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes);
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
  protected DatanodeDescriptor chooseLocalRack(Node localMachine,
                                             Set<Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeDescriptor> results,
                                             boolean avoidStaleNodes)
      throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes);
    }
      
    // choose one from the local rack
    try {
      return chooseRandom(localMachine.getNetworkLocation(), excludedNodes,
          blocksize, maxNodesPerRack, results, avoidStaleNodes);
    } catch (NotEnoughReplicasException e1) {
      // find the second replica
      DatanodeDescriptor newLocal=null;
      for(DatanodeDescriptor nextNode : results) {
        if (nextNode != localMachine) {
          newLocal = nextNode;
          break;
        }
      }
      if (newLocal != null) {
        try {
          return chooseRandom(newLocal.getNetworkLocation(), excludedNodes,
              blocksize, maxNodesPerRack, results, avoidStaleNodes);
        } catch(NotEnoughReplicasException e2) {
          //otherwise randomly choose one from the network
          return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes);
        }
      } else {
        //otherwise randomly choose one from the network
        return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes);
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
                                List<DatanodeDescriptor> results,
                                boolean avoidStaleNodes)
                                    throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // randomly choose one node from remote racks
    try {
      chooseRandom(numOfReplicas, "~" + localMachine.getNetworkLocation(),
          excludedNodes, blocksize, maxReplicasPerRack, results,
          avoidStaleNodes);
    } catch (NotEnoughReplicasException e) {
      chooseRandom(numOfReplicas-(results.size()-oldNumOfReplicas),
                   localMachine.getNetworkLocation(), excludedNodes, blocksize, 
                   maxReplicasPerRack, results, avoidStaleNodes);
    }
  }

  /**
   * Randomly choose one target from the given <i>scope</i>.
   * @return the chosen node, if there is any.
   */
  protected DatanodeDescriptor chooseRandom(String scope,
      Set<Node> excludedNodes,
      long blocksize,
      int maxNodesPerRack,
      List<DatanodeDescriptor> results,
      boolean avoidStaleNodes)
          throws NotEnoughReplicasException {
    return chooseRandom(1, scope, excludedNodes, blocksize, maxNodesPerRack,
        results, avoidStaleNodes);
  }

  /**
   * Randomly choose <i>numOfReplicas</i> targets from the given <i>scope</i>.
   * @return the first chosen node, if there is any.
   */
  protected DatanodeDescriptor chooseRandom(int numOfReplicas,
                            String scope,
                            Set<Node> excludedNodes,
                            long blocksize,
                            int maxNodesPerRack,
                            List<DatanodeDescriptor> results,
                            boolean avoidStaleNodes)
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
    DatanodeDescriptor firstChosen = null;
    while(numOfReplicas > 0 && numOfAvailableNodes > 0) {
      DatanodeDescriptor chosenNode = 
          (DatanodeDescriptor)clusterMap.chooseRandom(scope);
      if (excludedNodes.add(chosenNode)) { //was not in the excluded list
        numOfAvailableNodes--;

        int newExcludedNodes = addIfIsGoodTarget(chosenNode, excludedNodes,
            blocksize, maxNodesPerRack, considerLoad, results, avoidStaleNodes);
        if (newExcludedNodes >= 0) {
          numOfReplicas--;
          if (firstChosen == null) {
            firstChosen = chosenNode;
          }
          numOfAvailableNodes -= newExcludedNodes;
        } else {
          badTarget = true;
        }
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
   * If the given node is a good target, add it to the result list and
   * update the set of excluded nodes.
   * @return -1 if the given is not a good target;
   *         otherwise, return the number of nodes added to excludedNodes set.
   */
  int addIfIsGoodTarget(DatanodeDescriptor node,
      Set<Node> excludedNodes,
      long blockSize,
      int maxNodesPerRack,
      boolean considerLoad,
      List<DatanodeDescriptor> results,
      boolean avoidStaleNodes) {
    if (isGoodTarget(node, blockSize, maxNodesPerRack, considerLoad,
        results, avoidStaleNodes)) {
      results.add(node);
      // add node and related nodes to excludedNode
      return addToExcludedNodes(node, excludedNodes);
    } else { 
      return -1;
    }
  }

  private static void logNodeIsNotChosen(DatanodeDescriptor node, String reason) {
    if (LOG.isDebugEnabled()) {
      // build the error message for later use.
      debugLoggingBuilder.get()
          .append(node).append(": ")
          .append("Node ").append(NodeBase.getPath(node))
          .append(" is not chosen because ")
          .append(reason);
    }
  }

  /**
   * Determine if a node is a good target. 
   * 
   * @param node The target node
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
  private boolean isGoodTarget(DatanodeDescriptor node,
                               long blockSize, int maxTargetPerRack,
                               boolean considerLoad,
                               List<DatanodeDescriptor> results,                           
                               boolean avoidStaleNodes) {
    // check if the node is (being) decommissed
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      logNodeIsNotChosen(node, "the node is (being) decommissioned ");
      return false;
    }

    if (avoidStaleNodes) {
      if (node.isStale(this.staleInterval)) {
        logNodeIsNotChosen(node, "the node is stale ");
        return false;
      }
    }
    
    long remaining = node.getRemaining() - 
                     (node.getBlocksScheduled() * blockSize); 
    // check the remaining capacity of the target machine
    if (blockSize* HdfsConstants.MIN_BLOCKS_FOR_WRITE>remaining) {
      logNodeIsNotChosen(node, "the node does not have enough space ");
      return false;
    }
      
    // check the communication traffic of the target machine
    if (considerLoad) {
      double avgLoad = 0;
      int size = clusterMap.getNumOfLeaves();
      if (size != 0 && stats != null) {
        avgLoad = (double)stats.getTotalLoad()/size;
      }
      if (node.getXceiverCount() > (2.0 * avgLoad)) {
        logNodeIsNotChosen(node, "the node is too busy ");
        return false;
      }
    }
      
    // check if the target rack has chosen too many nodes
    String rackname = node.getNetworkLocation();
    int counter=1;
    for(Node result : results) {
      if (rackname.equals(result.getNetworkLocation())) {
        counter++;
      }
    }
    if (counter>maxTargetPerRack) {
      logNodeIsNotChosen(node, "the rack has too many chosen nodes ");
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
  private DatanodeDescriptor[] getPipeline(Node writer,
                                           DatanodeDescriptor[] nodes) {
    if (nodes.length==0) return nodes;
      
    synchronized(clusterMap) {
      int index=0;
      if (writer == null || !clusterMap.contains(writer)) {
        writer = nodes[0];
      }
      for(;index<nodes.length; index++) {
        DatanodeDescriptor shortestNode = nodes[index];
        int shortestDistance = clusterMap.getDistance(writer, shortestNode);
        int shortestIndex = index;
        for(int i=index+1; i<nodes.length; i++) {
          DatanodeDescriptor currentNode = nodes[i];
          int currentDistance = clusterMap.getDistance(writer, currentNode);
          if (shortestDistance>currentDistance) {
            shortestDistance = currentDistance;
            shortestNode = currentNode;
            shortestIndex = i;
          }
        }
        //switch position index & shortestIndex
        if (index != shortestIndex) {
          nodes[shortestIndex] = nodes[index];
          nodes[index] = shortestNode;
        }
        writer = shortestNode;
      }
    }
    return nodes;
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
  public DatanodeDescriptor chooseReplicaToDelete(BlockCollection bc,
      Block block, short replicationFactor,
      Collection<DatanodeDescriptor> first,
      Collection<DatanodeDescriptor> second) {
    long oldestHeartbeat =
      now() - heartbeatInterval * tolerateHeartbeatMultiplier;
    DatanodeDescriptor oldestHeartbeatNode = null;
    long minSpace = Long.MAX_VALUE;
    DatanodeDescriptor minSpaceNode = null;

    // Pick the node with the oldest heartbeat or with the least free space,
    // if all hearbeats are within the tolerable heartbeat interval
    for(DatanodeDescriptor node : pickupReplicaSet(first, second)) {
      long free = node.getRemaining();
      long lastHeartbeat = node.getLastUpdate();
      if(lastHeartbeat < oldestHeartbeat) {
        oldestHeartbeat = lastHeartbeat;
        oldestHeartbeatNode = node;
      }
      if (minSpace > free) {
        minSpace = free;
        minSpaceNode = node;
      }
    }
    return oldestHeartbeatNode != null ? oldestHeartbeatNode : minSpaceNode;
  }

  /**
   * Pick up replica node set for deleting replica as over-replicated. 
   * First set contains replica nodes on rack with more than one
   * replica while second set contains remaining replica nodes.
   * So pick up first set if not empty. If first is empty, then pick second.
   */
  protected Collection<DatanodeDescriptor> pickupReplicaSet(
      Collection<DatanodeDescriptor> first,
      Collection<DatanodeDescriptor> second) {
    return first.isEmpty() ? second : first;
  }
  
  @VisibleForTesting
  void setPreferLocalNode(boolean prefer) {
    this.preferLocalNode = prefer;
  }
}

