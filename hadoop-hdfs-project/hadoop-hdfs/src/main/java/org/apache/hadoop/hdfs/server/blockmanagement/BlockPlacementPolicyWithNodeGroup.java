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

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NetworkTopologyWithNodeGroup;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

/** The class is responsible for choosing the desired number of targets
 * for placing block replicas on environment with node-group layer.
 * The replica placement strategy is adjusted to:
 * If the writer is on a datanode, the 1st replica is placed on the local 
 *     node(or local node-group or on local rack), otherwise a random datanode.
 * The 2nd replica is placed on a datanode that is on a different rack with 1st
 *     replica node. 
 * The 3rd replica is placed on a datanode which is on a different node-group
 *     but the same rack as the second replica node.
 */
public class BlockPlacementPolicyWithNodeGroup extends BlockPlacementPolicyDefault {

  protected BlockPlacementPolicyWithNodeGroup() {
  }

  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
          NetworkTopology clusterMap, 
          Host2NodesMap host2datanodeMap) {
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
  }

  /**
   * choose all good favored nodes as target.
   * If no enough targets, then choose one replica from
   * each bad favored node's node group.
   * @throws NotEnoughReplicasException
   */
  @Override
  protected void chooseFavouredNodes(String src, int numOfReplicas,
      List<DatanodeDescriptor> favoredNodes,
      Set<Node> favoriteAndExcludedNodes, long blocksize,
      int maxNodesPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes, EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    super.chooseFavouredNodes(src, numOfReplicas, favoredNodes,
        favoriteAndExcludedNodes, blocksize, maxNodesPerRack, results,
        avoidStaleNodes, storageTypes);
    if (results.size() < numOfReplicas) {
      // Not enough replicas, choose from unselected Favorednode's Nodegroup
      for (int i = 0;
          i < favoredNodes.size() && results.size() < numOfReplicas; i++) {
        DatanodeDescriptor favoredNode = favoredNodes.get(i);
        boolean chosenNode =
            isNodeChosen(results, favoredNode);
        if (chosenNode) {
          continue;
        }
        NetworkTopologyWithNodeGroup clusterMapNodeGroup =
            (NetworkTopologyWithNodeGroup) clusterMap;
        // try a node on FavouredNode's node group
        DatanodeStorageInfo target = null;
        String scope =
            clusterMapNodeGroup.getNodeGroup(favoredNode.getNetworkLocation());
        try {
          target =
              chooseRandom(scope, favoriteAndExcludedNodes, blocksize,
                maxNodesPerRack, results, avoidStaleNodes, storageTypes);
        } catch (NotEnoughReplicasException e) {
          // catch Exception and continue with other favored nodes
          continue;
        }
        if (target == null) {
          LOG.warn("Could not find a target for file "
              + src + " within nodegroup of favored node " + favoredNode);
          continue;
        }
        favoriteAndExcludedNodes.add(target.getDatanodeDescriptor());
      }
    }
  }

  private boolean isNodeChosen(
      List<DatanodeStorageInfo> results, DatanodeDescriptor favoredNode) {
    boolean chosenNode = false;
    for (int j = 0; j < results.size(); j++) {
      if (results.get(j).getDatanodeDescriptor().equals(favoredNode)) {
        chosenNode = true;
        break;
      }
    }
    return chosenNode;
  }

  /** choose local node of <i>localMachine</i> as the target.
   * If localMachine is not available, will fallback to nodegroup/rack
   * when flag <i>fallbackToNodeGroupAndLocalRack</i> is set.
   * @return the chosen node
   */
  @Override
  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes,
      boolean fallbackToNodeGroupAndLocalRack)
      throws NotEnoughReplicasException {
    DatanodeStorageInfo localStorage = chooseLocalStorage(localMachine,
        excludedNodes, blocksize, maxNodesPerRack, results,
        avoidStaleNodes, storageTypes);
    if (localStorage != null) {
      return localStorage;
    }

    if (!fallbackToNodeGroupAndLocalRack) {
      return null;
    }
    // try a node on local node group
    DatanodeStorageInfo chosenStorage = chooseLocalNodeGroup(
        (NetworkTopologyWithNodeGroup)clusterMap, localMachine, excludedNodes, 
        blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    if (chosenStorage != null) {
      return chosenStorage;
    }
    // try a node on local rack
    return chooseLocalRack(localMachine, excludedNodes, 
        blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
  }

  /** @return the node of the second replica */
  private static DatanodeDescriptor secondNode(Node localMachine,
      List<DatanodeStorageInfo> results) {
    // find the second replica
    for(DatanodeStorageInfo nextStorage : results) {
      DatanodeDescriptor nextNode = nextStorage.getDatanodeDescriptor();
      if (nextNode != localMachine) {
        return nextNode;
      }
    }
    return null;
  }

  @Override
  protected DatanodeStorageInfo chooseLocalRack(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes) throws
      NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }

    // choose one from the local rack, but off-nodegroup
    try {
      final String scope = NetworkTopology.getFirstHalf(localMachine.getNetworkLocation());
      return chooseRandom(scope, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e1) {
      // find the second replica
      final DatanodeDescriptor newLocal = secondNode(localMachine, results);
      if (newLocal != null) {
        try {
          return chooseRandom(
              clusterMap.getRack(newLocal.getNetworkLocation()), excludedNodes,
              blocksize, maxNodesPerRack, results, avoidStaleNodes,
              storageTypes);
        } catch(NotEnoughReplicasException e2) {
          //otherwise randomly choose one from the network
          return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes, storageTypes);
        }
      } else {
        //otherwise randomly choose one from the network
        return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes, storageTypes);
      }
    }
  }

  @Override
  protected void chooseRemoteRack(int numOfReplicas,
      DatanodeDescriptor localMachine, Set<Node> excludedNodes,
      long blocksize, int maxReplicasPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes, EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();

    final String rackLocation = NetworkTopology.getFirstHalf(
        localMachine.getNetworkLocation());
    try {
      // randomly choose from remote racks
      chooseRandom(numOfReplicas, "~" + rackLocation, excludedNodes, blocksize,
          maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      // fall back to the local rack
      chooseRandom(numOfReplicas - (results.size() - oldNumOfReplicas),
          rackLocation, excludedNodes, blocksize,
          maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  /* choose one node from the nodegroup that <i>localMachine</i> is on.
   * if no such node is available, choose one node from the nodegroup where
   * a second replica is on.
   * if still no such node is available, return null.
   * @return the chosen node
   */
  private DatanodeStorageInfo chooseLocalNodeGroup(
      NetworkTopologyWithNodeGroup clusterMap, Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes) throws
      NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }

    // choose one from the local node group
    try {
      return chooseRandom(
          clusterMap.getNodeGroup(localMachine.getNetworkLocation()),
          excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes,
          storageTypes);
    } catch (NotEnoughReplicasException e1) {
      final DatanodeDescriptor newLocal = secondNode(localMachine, results);
      if (newLocal != null) {
        try {
          return chooseRandom(
              clusterMap.getNodeGroup(newLocal.getNetworkLocation()),
              excludedNodes, blocksize, maxNodesPerRack, results,
              avoidStaleNodes, storageTypes);
        } catch(NotEnoughReplicasException e2) {
          //otherwise return null
          return null;
        }
      } else {
        //otherwise return null
        return null;
      }
    }
  }

  @Override
  protected String getRack(final DatanodeInfo cur) {
    String nodeGroupString = cur.getNetworkLocation();
    return NetworkTopology.getFirstHalf(nodeGroupString);
  }
  
  /**
   * Find other nodes in the same nodegroup of <i>localMachine</i> and add them
   * into <i>excludeNodes</i> as replica should not be duplicated for nodes 
   * within the same nodegroup
   * @return number of new excluded nodes
   */
  @Override
  protected int addToExcludedNodes(DatanodeDescriptor chosenNode,
      Set<Node> excludedNodes) {
    int countOfExcludedNodes = 0;
    String nodeGroupScope = chosenNode.getNetworkLocation();
    List<Node> leafNodes = clusterMap.getLeaves(nodeGroupScope);
    for (Node leafNode : leafNodes) {
      if (excludedNodes.add(leafNode)) {
        // not a existing node in excludedNodes
        countOfExcludedNodes++;
      }
    }
    
    countOfExcludedNodes += addDependentNodesToExcludedNodes(
        chosenNode, excludedNodes);
    return countOfExcludedNodes;
  }
  
  /**
   * Add all nodes from a dependent nodes list to excludedNodes.
   * @return number of new excluded nodes
   */
  private int addDependentNodesToExcludedNodes(DatanodeDescriptor chosenNode,
      Set<Node> excludedNodes) {
    if (this.host2datanodeMap == null) {
      return 0;
    }
    int countOfExcludedNodes = 0;
    for(String hostname : chosenNode.getDependentHostNames()) {
      DatanodeDescriptor node =
          this.host2datanodeMap.getDataNodeByHostName(hostname);
      if(node!=null) {
        if (excludedNodes.add(node)) {
          countOfExcludedNodes++;
        }
      } else {
        LOG.warn("Not able to find datanode " + hostname
            + " which has dependency with datanode "
            + chosenNode.getHostName());
      }
    }
    
    return countOfExcludedNodes;
  }

  /**
   * Pick up replica node set for deleting replica as over-replicated. 
   * First set contains replica nodes on rack with more than one
   * replica while second set contains remaining replica nodes.
   * If first is not empty, divide first set into two subsets:
   *   moreThanOne contains nodes on nodegroup with more than one replica
   *   exactlyOne contains the remaining nodes in first set
   * then pickup priSet if not empty.
   * If first is empty, then pick second.
   */
  @Override
  public Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> first,
      Collection<DatanodeStorageInfo> second,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    // If no replica within same rack, return directly.
    if (first.isEmpty()) {
      return second;
    }
    // Split data nodes in the first set into two sets, 
    // moreThanOne contains nodes on nodegroup with more than one replica
    // exactlyOne contains the remaining nodes
    Map<String, List<DatanodeStorageInfo>> nodeGroupMap = new HashMap<>();
    
    for(DatanodeStorageInfo storage : first) {
      final String nodeGroupName = NetworkTopology.getLastHalf(
          storage.getDatanodeDescriptor().getNetworkLocation());
      List<DatanodeStorageInfo> storageList = nodeGroupMap.get(nodeGroupName);
      if (storageList == null) {
        storageList = new ArrayList<>();
        nodeGroupMap.put(nodeGroupName, storageList);
      }
      storageList.add(storage);
    }
    
    final List<DatanodeStorageInfo> moreThanOne = new ArrayList<>();
    final List<DatanodeStorageInfo> exactlyOne = new ArrayList<>();
    // split nodes into two sets
    for(List<DatanodeStorageInfo> datanodeList : nodeGroupMap.values()) {
      if (datanodeList.size() == 1 ) {
        // exactlyOne contains nodes on nodegroup with exactly one replica
        exactlyOne.add(datanodeList.get(0));
      } else {
        // moreThanOne contains nodes on nodegroup with more than one replica
        moreThanOne.addAll(datanodeList);
      }
    }
    
    return moreThanOne.isEmpty()? exactlyOne : moreThanOne;
  }

  /**
   * Check if there are any replica (other than source) on the same node group
   * with target. If true, then target is not a good candidate for placing
   * specific replica as we don't want 2 replicas under the same nodegroup.
   *
   * @return true if there are any replica (other than source) on the same node
   *         group with target
   */
  @Override
  public boolean isMovable(Collection<DatanodeInfo> locs,
      DatanodeInfo source, DatanodeInfo target) {
    for (DatanodeInfo dn : locs) {
      if (dn != source && dn != target &&
          clusterMap.isOnSameNodeGroup(dn, target)) {
        return false;
      }
    }
    return true;
  }


  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas) {
    if (locs == null) {
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    }

    List<String> locList = new ArrayList<String>();
    /*
     * remove the part of node group for BlockPlacementPolicyDefault to count
     * distinct racks, e.g. "/d1/r1/n1" --> "/d1/r1"
     */
    for (int i = 0; i < locs.length; i++) {
      locList.add(locs[i].getNetworkLocation());
      locs[i].setNetworkLocation(NetworkTopology.getFirstHalf(locs[i]
          .getNetworkLocation()));
    }

    BlockPlacementStatus defaultStatus = super.verifyBlockPlacement(locs,
        numberOfReplicas);

    // restore the part of node group back
    for (int i = 0; i < locs.length; i++) {
      locs[i].setNetworkLocation(locList.get(i));
    }

    int minNodeGroups = numberOfReplicas;
    BlockPlacementStatusWithNodeGroup nodeGroupStatus =
        new BlockPlacementStatusWithNodeGroup(
            defaultStatus, getNodeGroupsFromNode(locs), minNodeGroups);
    return nodeGroupStatus;
  }

  private Set<String> getNodeGroupsFromNode(DatanodeInfo[] nodes) {
    Set<String> nodeGroups = new HashSet<>();
    if (nodes == null) {
      return nodeGroups;
    }

    for (DatanodeInfo node : nodes) {
      nodeGroups.add(NetworkTopology.getLastHalf(node.getNetworkLocation()));
    }
    return nodeGroups;
  }
}
