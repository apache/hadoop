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

import org.apache.commons.logging.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import java.util.*;

/** The class is responsible for choosing the desired number of targets
 * for placing block replicas.
 * The replica placement strategy is that if the writer is on a datanode,
 * the 1st replica is placed on the local machine, 
 * otherwise a random datanode. The 2nd replica is placed on a datanode
 * that is on a different rack. The 3rd replica is placed on a datanode
 * which is on a different node of the rack as the second replica.
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyDefault extends BlockPlacementPolicy {
  private boolean considerLoad; 
  private NetworkTopology clusterMap;
  private FSClusterStats stats;

  BlockPlacementPolicyDefault(Configuration conf,  FSClusterStats stats,
                           NetworkTopology clusterMap) {
    initialize(conf, stats, clusterMap);
  }

  BlockPlacementPolicyDefault() {
  }
    
  /** {@inheritDoc} */
  public void initialize(Configuration conf,  FSClusterStats stats,
                         NetworkTopology clusterMap) {
    this.considerLoad = conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, true);
    this.stats = stats;
    this.clusterMap = clusterMap;
  }

  /** {@inheritDoc} */
  public DatanodeDescriptor[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    long blocksize) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, null, blocksize);
  }

  /** {@inheritDoc} */
  public DatanodeDescriptor[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    HashMap<Node, Node> excludedNodes,
                                    long blocksize) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, excludedNodes, blocksize);
  }


  /** {@inheritDoc} */
  @Override
  public DatanodeDescriptor[] chooseTarget(FSInodeInfo srcInode,
                                    int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    long blocksize) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, null, blocksize);
  }
    
  /**
   * This is not part of the public API but is used by the unit tests.
   */
  DatanodeDescriptor[] chooseTarget(int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    HashMap<Node, Node> excludedNodes,
                                    long blocksize) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return new DatanodeDescriptor[0];
    }
      
    if (excludedNodes == null) {
      excludedNodes = new HashMap<Node, Node>();
    }
     
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = chosenNodes.size()+numOfReplicas;
    if (totalNumOfReplicas > clusterSize) {
      numOfReplicas -= (totalNumOfReplicas-clusterSize);
      totalNumOfReplicas = clusterSize;
    }
      
    int maxNodesPerRack = 
      (totalNumOfReplicas-1)/clusterMap.getNumOfRacks()+2;
      
    List<DatanodeDescriptor> results = 
      new ArrayList<DatanodeDescriptor>(chosenNodes);
    for (Node node:chosenNodes) {
      excludedNodes.put(node, node);
    }
      
    if (!clusterMap.contains(writer)) {
      writer=null;
    }
      
    DatanodeDescriptor localNode = chooseTarget(numOfReplicas, writer, 
                                                excludedNodes, blocksize, maxNodesPerRack, results);
      
    results.removeAll(chosenNodes);
      
    // sorting nodes to form a pipeline
    return getPipeline((writer==null)?localNode:writer,
                       results.toArray(new DatanodeDescriptor[results.size()]));
  }
    
  /* choose <i>numOfReplicas</i> from all data nodes */
  private DatanodeDescriptor chooseTarget(int numOfReplicas,
                                          DatanodeDescriptor writer,
                                          HashMap<Node, Node> excludedNodes,
                                          long blocksize,
                                          int maxNodesPerRack,
                                          List<DatanodeDescriptor> results) {
      
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return writer;
    }
      
    int numOfResults = results.size();
    boolean newBlock = (numOfResults==0);
    if (writer == null && !newBlock) {
      writer = results.get(0);
    }
      
    try {
      if (numOfResults == 0) {
        writer = chooseLocalNode(writer, excludedNodes, 
                                 blocksize, maxNodesPerRack, results);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      if (numOfResults <= 1) {
        chooseRemoteRack(1, results.get(0), excludedNodes, 
                         blocksize, maxNodesPerRack, results);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      if (numOfResults <= 2) {
        if (clusterMap.isOnSameRack(results.get(0), results.get(1))) {
          chooseRemoteRack(1, results.get(0), excludedNodes,
                           blocksize, maxNodesPerRack, results);
        } else if (newBlock){
          chooseLocalRack(results.get(1), excludedNodes, blocksize, 
                          maxNodesPerRack, results);
        } else {
          chooseLocalRack(writer, excludedNodes, blocksize,
                          maxNodesPerRack, results);
        }
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, 
                   blocksize, maxNodesPerRack, results);
    } catch (NotEnoughReplicasException e) {
      FSNamesystem.LOG.warn("Not able to place enough replicas, still in need of "
               + numOfReplicas);
    }
    return writer;
  }
    
  /* choose <i>localMachine</i> as the target.
   * if <i>localMachine</i> is not available, 
   * choose a node on the same rack
   * @return the chosen node
   */
  private DatanodeDescriptor chooseLocalNode(
                                             DatanodeDescriptor localMachine,
                                             HashMap<Node, Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeDescriptor> results)
    throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
    if (localMachine == null)
      return chooseRandom(NodeBase.ROOT, excludedNodes, 
                          blocksize, maxNodesPerRack, results);
      
    // otherwise try local machine first
    Node oldNode = excludedNodes.put(localMachine, localMachine);
    if (oldNode == null) { // was not in the excluded list
      if (isGoodTarget(localMachine, blocksize,
                       maxNodesPerRack, false, results)) {
        results.add(localMachine);
        return localMachine;
      }
    } 
      
    // try a node on local rack
    return chooseLocalRack(localMachine, excludedNodes, 
                           blocksize, maxNodesPerRack, results);
  }
    
  /* choose one node from the rack that <i>localMachine</i> is on.
   * if no such node is available, choose one node from the rack where
   * a second replica is on.
   * if still no such node is available, choose a random node 
   * in the cluster.
   * @return the chosen node
   */
  private DatanodeDescriptor chooseLocalRack(
                                             DatanodeDescriptor localMachine,
                                             HashMap<Node, Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeDescriptor> results)
    throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, 
                          blocksize, maxNodesPerRack, results);
    }
      
    // choose one from the local rack
    try {
      return chooseRandom(
                          localMachine.getNetworkLocation(),
                          excludedNodes, blocksize, maxNodesPerRack, results);
    } catch (NotEnoughReplicasException e1) {
      // find the second replica
      DatanodeDescriptor newLocal=null;
      for(Iterator<DatanodeDescriptor> iter=results.iterator();
          iter.hasNext();) {
        DatanodeDescriptor nextNode = iter.next();
        if (nextNode != localMachine) {
          newLocal = nextNode;
          break;
        }
      }
      if (newLocal != null) {
        try {
          return chooseRandom(
                              newLocal.getNetworkLocation(),
                              excludedNodes, blocksize, maxNodesPerRack, results);
        } catch(NotEnoughReplicasException e2) {
          //otherwise randomly choose one from the network
          return chooseRandom(NodeBase.ROOT, excludedNodes,
                              blocksize, maxNodesPerRack, results);
        }
      } else {
        //otherwise randomly choose one from the network
        return chooseRandom(NodeBase.ROOT, excludedNodes,
                            blocksize, maxNodesPerRack, results);
      }
    }
  }
    
  /* choose <i>numOfReplicas</i> nodes from the racks 
   * that <i>localMachine</i> is NOT on.
   * if not enough nodes are available, choose the remaining ones 
   * from the local rack
   */
    
  private void chooseRemoteRack(int numOfReplicas,
                                DatanodeDescriptor localMachine,
                                HashMap<Node, Node> excludedNodes,
                                long blocksize,
                                int maxReplicasPerRack,
                                List<DatanodeDescriptor> results)
    throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // randomly choose one node from remote racks
    try {
      chooseRandom(numOfReplicas, "~"+localMachine.getNetworkLocation(),
                   excludedNodes, blocksize, maxReplicasPerRack, results);
    } catch (NotEnoughReplicasException e) {
      chooseRandom(numOfReplicas-(results.size()-oldNumOfReplicas),
                   localMachine.getNetworkLocation(), excludedNodes, blocksize, 
                   maxReplicasPerRack, results);
    }
  }

  /* Randomly choose one target from <i>nodes</i>.
   * @return the chosen node
   */
  private DatanodeDescriptor chooseRandom(
                                          String nodes,
                                          HashMap<Node, Node> excludedNodes,
                                          long blocksize,
                                          int maxNodesPerRack,
                                          List<DatanodeDescriptor> results) 
    throws NotEnoughReplicasException {
    int numOfAvailableNodes =
      clusterMap.countNumOfAvailableNodes(nodes, excludedNodes.keySet());
    while(numOfAvailableNodes > 0) {
      DatanodeDescriptor chosenNode = 
        (DatanodeDescriptor)(clusterMap.chooseRandom(nodes));

      Node oldNode = excludedNodes.put(chosenNode, chosenNode);
      if (oldNode == null) { // choosendNode was not in the excluded list
        numOfAvailableNodes--;
        if (isGoodTarget(chosenNode, blocksize, maxNodesPerRack, results)) {
          results.add(chosenNode);
          return chosenNode;
        }
      }
    }

    throw new NotEnoughReplicasException(
        "Not able to place enough replicas");
  }
    
  /* Randomly choose <i>numOfReplicas</i> targets from <i>nodes</i>.
   */
  private void chooseRandom(int numOfReplicas,
                            String nodes,
                            HashMap<Node, Node> excludedNodes,
                            long blocksize,
                            int maxNodesPerRack,
                            List<DatanodeDescriptor> results)
    throws NotEnoughReplicasException {
      
    int numOfAvailableNodes =
      clusterMap.countNumOfAvailableNodes(nodes, excludedNodes.keySet());
    while(numOfReplicas > 0 && numOfAvailableNodes > 0) {
      DatanodeDescriptor chosenNode = 
        (DatanodeDescriptor)(clusterMap.chooseRandom(nodes));
      Node oldNode = excludedNodes.put(chosenNode, chosenNode);
      if (oldNode == null) {
        numOfAvailableNodes--;

        if (isGoodTarget(chosenNode, blocksize, maxNodesPerRack, results)) {
          numOfReplicas--;
          results.add(chosenNode);
        }
      }
    }
      
    if (numOfReplicas>0) {
      throw new NotEnoughReplicasException(
                                           "Not able to place enough replicas");
    }
  }
    
  /* judge if a node is a good target.
   * return true if <i>node</i> has enough space, 
   * does not have too much load, and the rack does not have too many nodes
   */
  private boolean isGoodTarget(DatanodeDescriptor node,
                               long blockSize, int maxTargetPerLoc,
                               List<DatanodeDescriptor> results) {
    return isGoodTarget(node, blockSize, maxTargetPerLoc,
                        this.considerLoad, results);
  }
    
  private boolean isGoodTarget(DatanodeDescriptor node,
                               long blockSize, int maxTargetPerLoc,
                               boolean considerLoad,
                               List<DatanodeDescriptor> results) {
    Log logr = FSNamesystem.LOG;
    // check if the node is (being) decommissed
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      logr.debug("Node "+NodeBase.getPath(node)+
                " is not chosen because the node is (being) decommissioned");
      return false;
    }

    long remaining = node.getRemaining() - 
                     (node.getBlocksScheduled() * blockSize); 
    // check the remaining capacity of the target machine
    if (blockSize* FSConstants.MIN_BLOCKS_FOR_WRITE>remaining) {
      logr.debug("Node "+NodeBase.getPath(node)+
                " is not chosen because the node does not have enough space");
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
        logr.debug("Node "+NodeBase.getPath(node)+
                  " is not chosen because the node is too busy");
        return false;
      }
    }
      
    // check if the target rack has chosen too many nodes
    String rackname = node.getNetworkLocation();
    int counter=1;
    for(Iterator<DatanodeDescriptor> iter = results.iterator();
        iter.hasNext();) {
      Node result = iter.next();
      if (rackname.equals(result.getNetworkLocation())) {
        counter++;
      }
    }
    if (counter>maxTargetPerLoc) {
      logr.debug("Node "+NodeBase.getPath(node)+
                " is not chosen because the rack has too many chosen nodes");
      return false;
    }
    return true;
  }
    
  /* Return a pipeline of nodes.
   * The pipeline is formed finding a shortest path that 
   * starts from the writer and traverses all <i>nodes</i>
   * This is basically a traveling salesman problem.
   */
  private DatanodeDescriptor[] getPipeline(
                                           DatanodeDescriptor writer,
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

  /** {@inheritDoc} */
  public int verifyBlockPlacement(String srcPath,
                                  LocatedBlock lBlk,
                                  int minRacks) {
    DatanodeInfo[] locs = lBlk.getLocations();
    if (locs == null)
      locs = new DatanodeInfo[0];
    int numRacks = clusterMap.getNumOfRacks();
    if(numRacks <= 1) // only one rack
      return 0;
    minRacks = Math.min(minRacks, numRacks);
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    Set<String> racks = new TreeSet<String>();
    for (DatanodeInfo dn : locs)
      racks.add(dn.getNetworkLocation());
    return minRacks - racks.size();
  }

  /** {@inheritDoc} */
  public DatanodeDescriptor chooseReplicaToDelete(FSInodeInfo inode,
                                                 Block block,
                                                 short replicationFactor,
                                                 Collection<DatanodeDescriptor> first, 
                                                 Collection<DatanodeDescriptor> second) {
    long minSpace = Long.MAX_VALUE;
    DatanodeDescriptor cur = null;

    // pick replica from the first Set. If first is empty, then pick replicas
    // from second set.
    Iterator<DatanodeDescriptor> iter =
          first.isEmpty() ? second.iterator() : first.iterator();

    // pick node with least free space
    while (iter.hasNext() ) {
      DatanodeDescriptor node = iter.next();
      long free = node.getRemaining();
      if (minSpace > free) {
        minSpace = free;
        cur = node;
      }
    }
    return cur;
  }
}

