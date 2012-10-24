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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

/**
 * The class is responsible for choosing the desired number of targets for
 * placing block replicas in azure. The replica placement strategy is that if
 * the writer is on a datanode, the 1st replica is placed on the local machine,
 * otherwise a random datanode. The 2nd replica is placed on a datanode that is
 * in a different fault and upgrade domain than the first node. The 3rd replica
 * is placed on a datanode which does not belong to the upgrade domains of the
 * first two replicas.
 */
public class AzureBlockPlacementPolicy extends BlockPlacementPolicyDefault {

  private static final int DEFAULT_MIN_FAULT_DOMAINS = 2;

  private static final int DEFAULT_MIN_UPGRADE_DOMAINS = 3;

  private static final int DEFAULT_MIN_RACKS = Math.max(
      DEFAULT_MIN_FAULT_DOMAINS, DEFAULT_MIN_UPGRADE_DOMAINS);

  private static final Log LOG = FSNamesystem.LOG;

  /**
   * Chooses a rack as per the placement policy for multiple replicas. Places
   * only one replica at a time. The placement of the next replica depends on
   * the placement of the previous replicas.
   * 
   * @param excludedNodes
   *          nodes that should be selected
   * @param blocksize
   *          size of block
   * @param maxReplicasPerRack
   * @param results
   *          selected nodes so far
   * @param numOfReplicas the number of replicas required         
   * @throws NotEnoughReplicasException
   */
  private void chooseRack(HashMap<Node, Node> excludedNodes, long blocksize,
      int maxReplicasPerRack, List<DatanodeDescriptor> results,
      int numOfReplicas) throws NotEnoughReplicasException {

    // if results.size is 0, this function chooses a rack randomly.
    // It makes more sense to call chooseLocalNode when there are no results at
    // all.
    assert (results != null);
    if (numOfReplicas <= 0) {
      return;
    }
    while (numOfReplicas > 0) {
      boolean placedReplica = false;
      ArrayList<DatanodeDescriptor> selectedNodes = selectNodes(excludedNodes,
          results);
      if (selectedNodes.size() > 0) {
        for (DatanodeDescriptor result : selectedNodes) {
          if (isGoodTarget(result, blocksize, maxReplicasPerRack, results)) {
            if (!excludedNodes.containsKey(result)) {
              numOfReplicas--;
              results.add(result);
              excludedNodes.put(result, result);
              placedReplica = true;
              break;
            }
          }
        }
      } 
      if (!placedReplica) {
        throw new NotEnoughReplicasException(
            "Not able to place enough replicas");
      }
    }
  }

  /**
   * Selects the potential list of dataNodes for a block as per the placement
   * policy. Nodes in excludeNodes list would not be selected. If fault and
   * upgrade domains sets are empty(can happen if rack mapping is incorrect) all
   * nodes except those in the excludedNodes would be selected.
   * 
   * @param excludedNodes
   *          nodes that should not be selected
   * @param results
   *          nodes that are already selected
   * @return potential list of dataNodes which would be further filtered by the
   *         caller.
   */
  private ArrayList<DatanodeDescriptor> selectNodes(
      HashMap<Node, Node> excludedNodes, List<DatanodeDescriptor> results) {
    assert (results != null);

    Set<String> upgradeDomains = new TreeSet<String>();

    Set<String> faultDomains = new TreeSet<String>();

    populateFaultAndUpgradeDomains(results, faultDomains,
        upgradeDomains);

    // Note that if upgradeDomains and faultDomains are empty
    // all nodes except those in excludedNodes would be selected by the code
    // below

    // ensure that the blocks are split across
    // at least DEFAULT_MIN_FAULT_DOMAINS fault domains and
    // DEFAULT_MIN_UPGRADE_DOMAINS upgrade domains
    boolean selectBasedOnFaultDomain = (faultDomains.size() < DEFAULT_MIN_FAULT_DOMAINS);

    boolean selectBasedOnUpgradeDomain = (upgradeDomains.size() < DEFAULT_MIN_UPGRADE_DOMAINS);

    List<Node> leaves = clusterMap.getLeaves();

    ArrayList<DatanodeDescriptor> selectedNodes = new ArrayList<DatanodeDescriptor>();

    for (Node node : leaves) {
      assert (node instanceof DatanodeDescriptor);

      DatanodeDescriptor dataNodeDescriptor = (DatanodeDescriptor) node;

      if (excludedNodes.get(dataNodeDescriptor) == null) {

        boolean isDataNodeSelected = true;

        TreeSet<String> currentFaultDomain = new TreeSet<String>();
        TreeSet<String> currentUpgradeDomain = new TreeSet<String>();

        if (selectBasedOnFaultDomain || selectBasedOnUpgradeDomain) {
          populateFaultAndUpgradeDomains(Arrays.asList(dataNodeDescriptor),
              currentFaultDomain, currentUpgradeDomain);
        }

        if (selectBasedOnFaultDomain) {
          // Do not select a data node in an existing fault domain
          for (String faultDomain : faultDomains) {
            // if currentFaultDomain is empty, then an incorrect mapping is
            // being used. just select
            // all nodes in that case.
            if ((!currentFaultDomain.isEmpty())
                && currentFaultDomain.first().equals(faultDomain)) {
              isDataNodeSelected = false;
              break;
            }
          }
        }

        if (isDataNodeSelected && selectBasedOnUpgradeDomain) {
          // Do not select a data node in an existing upgrade domain
          for (String upgradeDomain : upgradeDomains) {
            // if currentUpgradeDomain is empty, then an incorrect mapping is
            // being used. just select
            // all nodes in that case.
            if ((!currentUpgradeDomain.isEmpty())
                && currentUpgradeDomain.first().equals(upgradeDomain)) {
              isDataNodeSelected = false;
              break;
            }
          }
        }

        if (isDataNodeSelected) {
          selectedNodes.add(dataNodeDescriptor);
        }
      }
    }

    // We choose all possible nodes and then randomize them so that nodes
    // are picked randomly.
    Collections.shuffle(selectedNodes);

    return selectedNodes;
  }

  /**
   * Verify that the block is replicated on at least two fault domains and three
   * upgrade domains
   * 
   * @param srcPath
   *          the full pathname of the file to be verified
   * @param lBlk
   *          block with locations
   * @param replication
   *          target file replication
   * @return the difference between the required and the actual number of racks
   *         the block is replicated to.
   */
  @Override
  public int verifyBlockPlacement(String srcPath, LocatedBlock lBlk,
      short replication) {

    int minRacks = Math.min(DEFAULT_MIN_RACKS, replication);

    DatanodeInfo[] locs = lBlk.getLocations();
    if (locs == null)
      locs = new DatanodeInfo[0];
    int numRacks = clusterMap.getNumOfRacks();
    if (numRacks < DEFAULT_MIN_RACKS)
      return 0;
    minRacks = Math.min(minRacks, numRacks);

    // ensure that the blocks are split across
    // at least 2 fault domains and three upgrade domains
    Set<String> upgradeDomains = new TreeSet<String>();
    Set<String> faultDomains = new TreeSet<String>();
    populateFaultAndUpgradeDomains(Arrays.asList(locs), faultDomains, upgradeDomains);

    int requiredRacks = 0;
    if (faultDomains.size() < DEFAULT_MIN_FAULT_DOMAINS
        || upgradeDomains.size() < DEFAULT_MIN_UPGRADE_DOMAINS) {
      requiredRacks = Math.max(
          (DEFAULT_MIN_FAULT_DOMAINS - faultDomains.size()),
          (DEFAULT_MIN_UPGRADE_DOMAINS - upgradeDomains.size()));
    }

    if (requiredRacks < 0)
      requiredRacks = 0;

    return requiredRacks;
  }

  /**
   * Populate the faultDomains and upgradeDomains sets for a given set of data
   * nodes
   * 
   * @param list
   *          list of data nodes
   * @param faultDomains
   *          set of faultDomains for given data nodes. This will be populated
   *          by the method.
   * @param upgradeDomains
   *          set of upgradeDomains for given data nodes. This will be populated
   *          by the method.
   */
  private static void populateFaultAndUpgradeDomains(List<? extends DatanodeInfo> list,
      Set<String> faultDomains, Set<String> upgradeDomains) {
    assert (list != null);

    assert ((faultDomains != null) && (faultDomains.size() == 0));

    assert ((upgradeDomains != null) && (upgradeDomains.size() == 0));

    for (DatanodeInfo dn : list) {
      assert (dn != null);

      // The assumption here is that the network location is of the format
      // /fd1/ud1
      // get the node corresponding to the network location
      Node parent = dn.getParent();

      // get the node corresponding to the parent of the network location
      Node grandParent = null;

      if (parent != null) {
        grandParent = parent.getParent();
      }

      if (parent != null && grandParent != null
          && (!grandParent.getName().equals(NodeBase.ROOT))) {
        upgradeDomains.add(parent.getName());
        faultDomains.add(grandParent.getName());
      } else {
        LOG.warn("This placement policy is not meant for the current network location mapping: "
            + dn.getNetworkLocation());
      }
    }
  }

  /**
   * Determines whether datanodeToDelete can be deleted from the collection of
   * dataNodes that contain the replicas as per the placement policy.
   */
  protected boolean canDelete(DatanodeDescriptor datanodeToDelete,
      List<DatanodeDescriptor> dataNodes, short replication) {
    assert (datanodeToDelete != null);

    assert (dataNodes != null);

    // populate the fault domains and upgrade domains sets for the dataNodes
    // list
    Set<String> faultDomains = new TreeSet<String>();

    Set<String> upgradeDomains = new TreeSet<String>();

    populateFaultAndUpgradeDomains(dataNodes, faultDomains,
        upgradeDomains);

    // populate the fault domains and upgrade domains sets for list (dataNodes -
    // datanodeToDelete)
    List<DatanodeDescriptor> newDataNodes = new ArrayList<DatanodeDescriptor>();
    for (DatanodeDescriptor d : dataNodes) {
      if (d != datanodeToDelete)
        newDataNodes.add(d);
    }

    Set<String> newUpgradeDomains = new TreeSet<String>();

    Set<String> newFaultDomains = new TreeSet<String>();

    populateFaultAndUpgradeDomains(newDataNodes,
        newFaultDomains, newUpgradeDomains);

    int minFaultDomains = Math.min(DEFAULT_MIN_FAULT_DOMAINS, replication);
    int minUpgradeDomains = Math.min(DEFAULT_MIN_UPGRADE_DOMAINS, replication);

    if (
    // if the fault and upgrade domains count does not change
    // or is not less than the minimum requirement for fault and upgrade
    // domains by deleting the node, it can be deleted.
    ((newFaultDomains.size() >= Math.min(minFaultDomains, faultDomains.size())) && (newUpgradeDomains
        .size() >= Math.min(minUpgradeDomains, upgradeDomains.size())))) {
      return true;
    }

    return false;
  }

  /**
   * Chooses numOfReplicas for block placement
   * 
   * @param numOfReplicas
   * @param writer
   * @param excludedNodes
   *          Nodes that should not be used for selection
   * @param blocksize
   * @param maxNodesPerRack
   * @param results
   *          all nodes that already contain the block being replicated
   * @return
   */
  @Override
  protected DatanodeDescriptor chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer, HashMap<Node, Node> excludedNodes,
      long blocksize, int maxNodesPerRack, List<DatanodeDescriptor> results) {

    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves() == 0) {
      return writer;
    }

    int targetNum = numOfReplicas;
    int numOfResults = results.size();
    boolean newBlock = (numOfResults == 0);
    if (writer == null && !newBlock) {
      writer = (DatanodeDescriptor) results.get(0);
    }

    try {
      if (numOfResults == 0) {
        writer = chooseLocalNode(writer, excludedNodes, blocksize,
            maxNodesPerRack, results);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      chooseRack(excludedNodes, blocksize, maxNodesPerRack, results,
          numOfReplicas);
    } catch (NotEnoughReplicasException e) {
      LOG.warn("Not able to place enough replicas, still in need of "
          + (targetNum - results.size() + numOfResults));
    }
    return writer;
  }

  /** {@inheritDoc} */
  @Override
  public DatanodeDescriptor chooseReplicaToDelete(FSInodeInfo inode,
      Block block, short replicationFactor,
      Collection<DatanodeDescriptor> first,
      Collection<DatanodeDescriptor> second) {
    long minSpace = Long.MAX_VALUE;
    DatanodeDescriptor cur = null;

    assert first != null;
    assert second != null;

    List<DatanodeDescriptor> allReplicas = new ArrayList<DatanodeDescriptor>();
    allReplicas.addAll(first);
    allReplicas.addAll(second);

    // pick replica from the first Set. If first is empty, then pick replicas
    // from second set.
    Iterator<DatanodeDescriptor> iter = first.isEmpty() ? second.iterator()
        : first.iterator();

    // pick node with least free space
    while (iter.hasNext()) {
      DatanodeDescriptor node = iter.next();
      long free = node.getRemaining();
      // if a set
      if ((first.isEmpty() ? canDelete(node, allReplicas, replicationFactor)
          : true) && minSpace > free) {
        minSpace = free;
        cur = node;
      }
    }
    return cur;
  }

  /** {@inheritDoc} */
  @Override
  public boolean canMove(Block block, DatanodeInfo source, DatanodeInfo target,
      List<DatanodeInfo> dataNodes) {

    // populate the fault domains and upgrade domains sets for the dataNodes
    Set<String> faultDomains = new TreeSet<String>();
    Set<String> upgradeDomains = new TreeSet<String>();

    populateFaultAndUpgradeDomains(dataNodes, faultDomains,
        upgradeDomains);

    // populate the fault domains and upgrade domains sets for list (dataNodes -
    // source+target)
    List<DatanodeInfo> newDataNodes = new ArrayList<DatanodeInfo>();
    for (DatanodeInfo d : dataNodes) {
      if (d != source)
        newDataNodes.add(d);
    }
    newDataNodes.add(target);

    Set<String> newUpgradeDomains = new TreeSet<String>();
    Set<String> newFaultDomains = new TreeSet<String>();

    populateFaultAndUpgradeDomains(newDataNodes,
        newFaultDomains, newUpgradeDomains);

    if (
    // if the fault and upgrade domains count does not change
    // or is not less than the minimum requirement(or old counts)
    // for fault and upgrade
    // domains by moving to a new node, then the move is valid.
    ((newFaultDomains.size() >= Math.min(DEFAULT_MIN_FAULT_DOMAINS,
        faultDomains.size())) && (newUpgradeDomains.size() >= Math.min(
        DEFAULT_MIN_UPGRADE_DOMAINS, upgradeDomains.size())))) {
      return true;
    }

    return false;
  }
}
