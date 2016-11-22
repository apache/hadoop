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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.*;

/**
 * The class is responsible for choosing the desired number of targets
 * for placing block replicas in a GDA-aware fashion.
 * The strategy is that it tries its best to place the replicas to most racks
 * while keeping in mind the link costs associated with geo-distributed nodes.
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyRackFaultTolerantGDA extends BlockPlacementPolicyDefault {

  private int sameRackPenalty;

  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
                         NetworkTopology clusterMap, 
                         Host2NodesMap host2datanodeMap) {
    this.sameRackPenalty = conf.getInt(
        DFSConfigKeys.NET_LINK_SAME_RACK_PENALTY_KEY,
        DFSConfigKeys.NET_LINK_SAME_RACK_PENALTY_DEFAULT);
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
  }

  @Override
  protected int[] getMaxNodesPerRack(int numOfChosen, int numOfReplicas) {
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = numOfChosen + numOfReplicas;
    if (totalNumOfReplicas > clusterSize) {
      numOfReplicas -= (totalNumOfReplicas-clusterSize);
      totalNumOfReplicas = clusterSize;
    }
    // No calculation needed when there is only one rack or picking one node.
    int numOfRacks = clusterMap.getNumOfRacks();
    if (numOfRacks == 1 || totalNumOfReplicas <= 1) {
      return new int[] {numOfReplicas, totalNumOfReplicas};
    }
    // Don't set any restrictions. Let GDA decide the limit.
    return new int[] {numOfReplicas, totalNumOfReplicas};
  }

  /**
   * Choose numOfReplicas in order:
   * TODO kbavishi - Fill this up
   * @return local node of writer
   */
  @Override
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
    LOG.warn("Writer: " + writer + ", rack: " + writer.getNetworkLocation());
    if (numOfResults == 0) {
      writer = chooseLocalStorage(writer, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes, true)
          .getDatanodeDescriptor();
      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    chooseRandomGDA(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize,
                    maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    return writer;
  }

  /**
   * TODO kbavishi - Fill this up
   */
  protected DatanodeStorageInfo chooseRandomGDA(int numOfReplicas,
                            String scope,
                            Set<Node> excludedNodes,
                            long blocksize,
                            int maxNodesPerRack,
                            List<DatanodeStorageInfo> results,
                            boolean avoidStaleNodes,
                            EnumMap<StorageType, Integer> storageTypes)
                            throws NotEnoughReplicasException {
    StringBuilder builder = null;
    if (LOG.isDebugEnabled()) {
    }
    boolean badTarget = false;
    DatanodeStorageInfo firstChosen = null;

    // Assume that one replica has already been placed at local storage
    final DatanodeDescriptor dn0 = results.get(0).getDatanodeDescriptor();

    // 1. Find all racks
    ArrayList<String> racks = new ArrayList(clusterMap.getRackNames());

    // 2. Sort all racks by distance from rack hosting DN0
    TreeMap<Integer, List<String>> tree;
    tree = clusterMap.sortRacksByDistance(dn0, racks);

    // 3. Greedily pick the closest racks and pawn off one replica to a randomly
    // selected node on it.
    while (numOfReplicas > 0) {
      // Pick the rack with the least cost.
      Map.Entry<Integer, List<String>> treeEntry = tree.pollFirstEntry();
      int cost = treeEntry.getKey();
      List<String> rackNames = treeEntry.getValue();
      String chosenRack = rackNames.remove(0);
      LOG.warn("GDA: Picked rack " + chosenRack + " with cost " +  cost);

      DatanodeDescriptor chosenNode = chooseDataNode(chosenRack,
                                                     excludedNodes);
      LOG.warn("GDA: Yielded datanode " + chosenNode);
      if (chosenNode == null) {
        // Chosen rack is no good because it did not yield any data nodes.
        // Do not consider it again for selection.
        if (tree.size() == 0 && rackNames.size() == 0) {
          // No more racks left. Quit
          break;
        } else if (rackNames.size() == 0) {
          // No racks exist at the same cost. Don't add back cost entry
        } else {
          // Other racks exist with the same cost. Add them back to the tree.
          tree.put(cost, rackNames);
        }
        // Try again
        continue;

      } else {
        // Chosen rack generated a datanode. Reconsider it for selection after
        // updating with the same rack penalty.
        int updatedCost = cost + this.sameRackPenalty;
        List<String> list = tree.get(updatedCost);
        if (list == null) {
          list = Lists.newArrayListWithExpectedSize(1);
          tree.put(updatedCost, list);
        }
        list.add(chosenRack);

        if (rackNames.size() == 0) {
          // No racks exist at the previous cost. Don't add back cost entry
        } else {
          // Other racks exist with the same cost. Add them back to the tree.
          tree.put(cost, rackNames);
        }
      }

      Preconditions.checkState(excludedNodes.add(chosenNode), "chosenNode "
          + chosenNode + " is already in excludedNodes " + excludedNodes);

      DatanodeStorageInfo storage = null;
      if (isGoodDatanode(chosenNode, maxNodesPerRack, considerLoad,
          results, avoidStaleNodes)) {
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
            .entrySet().iterator(); iter.hasNext();) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          storage = chooseStorage4Block(
              chosenNode, blocksize, results, entry.getKey());
          if (storage != null) {
            numOfReplicas--;
            LOG.warn("GDA: Finally chosen node " + chosenNode);
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
        // If no candidate storage was found on this DN then set badTarget.
        badTarget = (storage == null);
      }
    }
    if (numOfReplicas > 0) {
      throw new NotEnoughReplicasException("Could not find enough GDA replicas");
    }
    return firstChosen;
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas) {
    if (locs == null)
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    if (!clusterMap.hasClusterEverBeenMultiRack()) {
      // only one rack
      return new BlockPlacementStatusDefault(1, 1, 1);
    }
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    Set<String> racks = new TreeSet<>();
    for (DatanodeInfo dn : locs) {
      racks.add(dn.getNetworkLocation());
    }
    return new BlockPlacementStatusDefault(racks.size(), numberOfReplicas,
        clusterMap.getNumOfRacks());
  }

  @Override
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    return moreThanOne.isEmpty() ? exactlyOne : moreThanOne;
  }
}
