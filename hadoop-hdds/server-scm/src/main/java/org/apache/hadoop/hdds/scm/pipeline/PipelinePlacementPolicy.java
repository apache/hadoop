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

package org.apache.hadoop.hdds.scm.pipeline;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMCommonPolicy;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Pipeline placement policy that choose datanodes based on load balancing
 * and network topology to supply pipeline creation.
 * <p>
 * 1. get a list of healthy nodes
 * 2. filter out nodes that are not too heavily engaged in other pipelines
 * 3. Choose an anchor node among the viable nodes.
 * 4. Choose other nodes around the anchor node based on network topology
 */
public final class PipelinePlacementPolicy extends SCMCommonPolicy {
  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(PipelinePlacementPolicy.class);
  private final NodeManager nodeManager;
  private final Configuration conf;
  private final int heavyNodeCriteria;

  /**
   * Constructs a pipeline placement with considering network topology,
   * load balancing and rack awareness.
   *
   * @param nodeManager Node Manager
   * @param conf        Configuration
   */
  public PipelinePlacementPolicy(
      final NodeManager nodeManager, final Configuration conf) {
    super(nodeManager, conf);
    this.nodeManager = nodeManager;
    this.conf = conf;
    heavyNodeCriteria = conf.getInt(
        ScmConfigKeys.OZONE_DATANODE_MAX_PIPELINE_ENGAGEMENT,
        ScmConfigKeys.OZONE_DATANODE_MAX_PIPELINE_ENGAGEMENT_DEFAULT);
  }

  /**
   * Returns true if this node meets the criteria.
   *
   * @param datanodeDetails DatanodeDetails
   * @return true if we have enough space.
   */
  @VisibleForTesting
  boolean meetCriteria(DatanodeDetails datanodeDetails, long heavyNodeLimit) {
    return (nodeManager.getPipelinesCount(datanodeDetails) <= heavyNodeLimit);
  }

  /**
   * Filter out viable nodes based on
   * 1. nodes that are healthy
   * 2. nodes that are not too heavily engaged in other pipelines
   *
   * @param excludedNodes - excluded nodes
   * @param nodesRequired - number of datanodes required.
   * @return a list of viable nodes
   * @throws SCMException when viable nodes are not enough in numbers
   */
  List<DatanodeDetails> filterViableNodes(
      List<DatanodeDetails> excludedNodes, int nodesRequired)
      throws SCMException {
    // get nodes in HEALTHY state
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY);
    if (excludedNodes != null) {
      healthyNodes.removeAll(excludedNodes);
    }
    String msg;
    if (healthyNodes.size() == 0) {
      msg = "No healthy node found to allocate pipeline.";
      LOG.error(msg);
      throw new SCMException(msg, SCMException.ResultCodes
          .FAILED_TO_FIND_HEALTHY_NODES);
    }

    if (healthyNodes.size() < nodesRequired) {
      msg = String.format("Not enough healthy nodes to allocate pipeline. %d "
              + " datanodes required. Found %d",
          nodesRequired, healthyNodes.size());
      LOG.error(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    // filter nodes that meet the size and pipeline engagement criteria.
    // Pipeline placement doesn't take node space left into account.
    List<DatanodeDetails> healthyList = healthyNodes.stream().filter(d ->
        meetCriteria(d, heavyNodeCriteria)).collect(Collectors.toList());

    if (healthyList.size() < nodesRequired) {
      msg = String.format("Unable to find enough nodes that meet " +
              "the criteria that cannot engage in more than %d pipelines." +
              " Nodes required: %d Found: %d",
          heavyNodeCriteria, nodesRequired, healthyList.size());
      LOG.error(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    return healthyList;
  }

  /**
   * Pipeline placement choose datanodes to join the pipeline.
   *
   * @param excludedNodes - excluded nodes
   * @param favoredNodes  - list of nodes preferred.
   * @param nodesRequired - number of datanodes required.
   * @param sizeRequired  - size required for the container or block.
   * @return a list of chosen datanodeDetails
   * @throws SCMException when chosen nodes are not enough in numbers
   */
  @Override
  public List<DatanodeDetails> chooseDatanodes(
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes,
      int nodesRequired, final long sizeRequired) throws SCMException {
    // get a list of viable nodes based on criteria
    List<DatanodeDetails> healthyNodes =
        filterViableNodes(excludedNodes, nodesRequired);

    List<DatanodeDetails> results = new ArrayList<>();

    // Randomly picks nodes when all nodes are equal.
    // This happens when network topology is absent or
    // all nodes are on the same rack.
    if (checkAllNodesAreEqual(nodeManager.getClusterNetworkTopologyMap())) {
      LOG.info("All nodes are considered equal. Now randomly pick nodes. " +
          "Required nodes: {}", nodesRequired);
      results = super.getResultSet(nodesRequired, healthyNodes);
      if (results.size() < nodesRequired) {
        LOG.error("Unable to find the required number of healthy nodes that " +
                "meet the criteria. Required nodes: {}, Found nodes: {}",
            nodesRequired, results.size());
        throw new SCMException("Unable to find required number of nodes.",
            SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
      }
      return results;
    }

    // Since nodes are widely distributed, the results should be selected
    // base on distance in topology, rack awareness and load balancing.
    List<DatanodeDetails> exclude = new ArrayList<>();
    exclude.addAll(excludedNodes);
    // First choose an anchor nodes randomly
    DatanodeDetails anchor = chooseNode(healthyNodes);
    if (anchor == null) {
      LOG.error("Unable to find the first healthy nodes that " +
              "meet the criteria. Required nodes: {}, Found nodes: {}",
          nodesRequired, results.size());
      throw new SCMException("Unable to find required number of nodes.",
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    results.add(anchor);
    exclude.add(anchor);
    nodesRequired--;

    // Choose the second node on different racks from anchor.
    DatanodeDetails nodeOnDifferentRack = chooseNodeBasedOnRackAwareness(
        healthyNodes, excludedNodes,
        nodeManager.getClusterNetworkTopologyMap(), anchor);
    if (nodeOnDifferentRack == null) {
      LOG.error("Unable to find nodes on different racks that " +
              "meet the criteria. Required nodes: {}, Found nodes: {}",
          nodesRequired, results.size());
      throw new SCMException("Unable to find required number of nodes.",
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    results.add(nodeOnDifferentRack);
    exclude.add(nodeOnDifferentRack);
    nodesRequired--;

    // Then choose nodes close to anchor based on network topology
    for (int x = 0; x < nodesRequired; x++) {
      // invoke the choose function defined in the derived classes.
      DatanodeDetails pick = chooseNodeFromNetworkTopology(
          nodeManager.getClusterNetworkTopologyMap(), anchor, exclude);
      if (pick != null) {
        results.add(pick);
        // exclude the picked node for next time
        exclude.add(pick);
      }
    }

    if (results.size() < nodesRequired) {
      LOG.error("Unable to find the required number of healthy nodes that " +
              "meet the criteria. Required nodes: {}, Found nodes: {}",
          nodesRequired, results.size());
      throw new SCMException("Unable to find required number of nodes.",
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    return results;
  }

  /**
   * Find a node from the healthy list and return it after removing it from the
   * list that we are operating on.
   *
   * @param healthyNodes - Set of healthy nodes we can choose from.
   * @return chosen datanodDetails
   */
  @Override
  public DatanodeDetails chooseNode(
      List<DatanodeDetails> healthyNodes) {
    int firstNodeNdx = getRand().nextInt(healthyNodes.size());
    int secondNodeNdx = getRand().nextInt(healthyNodes.size());

    DatanodeDetails datanodeDetails;
    // There is a possibility that both numbers will be same.
    // if that is so, we just return the node.
    if (firstNodeNdx == secondNodeNdx) {
      datanodeDetails = healthyNodes.get(firstNodeNdx);
    } else {
      DatanodeDetails firstNodeDetails = healthyNodes.get(firstNodeNdx);
      DatanodeDetails secondNodeDetails = healthyNodes.get(secondNodeNdx);
      SCMNodeMetric firstNodeMetric =
          nodeManager.getNodeStat(firstNodeDetails);
      SCMNodeMetric secondNodeMetric =
          nodeManager.getNodeStat(secondNodeDetails);
      datanodeDetails = firstNodeMetric.isGreater(secondNodeMetric.get())
          ? firstNodeDetails : secondNodeDetails;
    }
    // the pick is decided and it should be removed from candidates.
    healthyNodes.remove(datanodeDetails);
    return datanodeDetails;
  }

  /**
   * Choose node on different racks as anchor is on based on rack awareness.
   * If a node on different racks cannot be found, then return a random node.
   * @param healthyNodes healthy nodes
   * @param excludedNodes excluded nodes
   * @param networkTopology network topology
   * @param anchor anchor node
   * @return a node on different rack
   */
  @VisibleForTesting
  protected DatanodeDetails chooseNodeBasedOnRackAwareness(
      List<DatanodeDetails> healthyNodes,  List<DatanodeDetails> excludedNodes,
      NetworkTopology networkTopology, DatanodeDetails anchor) {
    Preconditions.checkArgument(networkTopology != null);
    if (checkAllNodesAreEqual(networkTopology)) {
      return null;
    }

    for (DatanodeDetails node : healthyNodes) {
      if (excludedNodes.contains(node)
          || networkTopology.isSameParent(anchor, node)) {
        continue;
      } else {
        // the pick is decided and it should be removed from candidates.
        healthyNodes.remove(node);
        return node;
      }
    }
    return null;
  }

  /**
   * Check if all nodes are equal in topology.
   * They are equal when network topology is absent or there are on
   * the same rack.
   * @param topology network topology
   * @return true when all nodes are equal
   */
  private boolean checkAllNodesAreEqual(NetworkTopology topology) {
    if (topology == null) {
      return true;
    }
    return (topology.getNumOfNodes(topology.getMaxLevel() - 1) == 1);
  }

  /**
   * Choose node based on network topology.
   * @param networkTopology network topology
   * @param anchor anchor datanode to start with
   * @param excludedNodes excluded datanodes
   * @return chosen datanode
   */
  @VisibleForTesting
  protected DatanodeDetails chooseNodeFromNetworkTopology(
      NetworkTopology networkTopology, DatanodeDetails anchor,
      List<DatanodeDetails> excludedNodes) {
    Preconditions.checkArgument(networkTopology != null);

    Collection<Node> excluded = new ArrayList<>();
    if (excludedNodes != null && excludedNodes.size() != 0) {
      excluded.addAll(excludedNodes);
    }
    excluded.add(anchor);

    Node pick = networkTopology.chooseRandom(
        anchor.getNetworkLocation(), excluded);
    DatanodeDetails pickedNode = (DatanodeDetails) pick;
    // exclude the picked node for next time
    if (excludedNodes != null) {
      excludedNodes.add(pickedNode);
    }
    return pickedNode;
  }
}
