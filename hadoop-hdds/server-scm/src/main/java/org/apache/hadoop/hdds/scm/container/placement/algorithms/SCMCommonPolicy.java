/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * SCM CommonPolicy implements a set of invariants which are common
 * for all container placement policies, acts as the repository of helper
 * functions which are common to placement policies.
 */
public abstract class SCMCommonPolicy implements ContainerPlacementPolicy {
  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(SCMCommonPolicy.class);
  private final NodeManager nodeManager;
  private final Random rand;
  private final Configuration conf;

  /**
   * Constructs SCM Common Policy Class.
   *
   * @param nodeManager NodeManager
   * @param conf Configuration class.
   */
  public SCMCommonPolicy(NodeManager nodeManager, Configuration conf) {
    this.nodeManager = nodeManager;
    this.rand = new Random();
    this.conf = conf;
  }

  /**
   * Return node manager.
   *
   * @return node manager
   */
  public NodeManager getNodeManager() {
    return nodeManager;
  }

  /**
   * Returns the Random Object.
   *
   * @return rand
   */
  public Random getRand() {
    return rand;
  }

  /**
   * Get Config.
   *
   * @return Configuration
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Given the replication factor and size required, return set of datanodes
   * that satisfy the nodes and size requirement.
   * <p>
   * Here are some invariants of container placement.
   * <p>
   * 1. We place containers only on healthy nodes.
   * 2. We place containers on nodes with enough space for that container.
   * 3. if a set of containers are requested, we either meet the required
   * number of nodes or we fail that request.
   *
   *
   * @param excludedNodes - datanodes with existing replicas
   * @param nodesRequired - number of datanodes required.
   * @param sizeRequired - size required for the container or block.
   * @return list of datanodes chosen.
   * @throws SCMException SCM exception.
   */
  @Override
  public List<DatanodeDetails> chooseDatanodes(
      List<DatanodeDetails> excludedNodes,
      int nodesRequired, final long sizeRequired) throws SCMException {
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY);
    healthyNodes.removeAll(excludedNodes);
    String msg;
    if (healthyNodes.size() == 0) {
      msg = "No healthy node found to allocate container.";
      LOG.error(msg);
      throw new SCMException(msg, SCMException.ResultCodes
          .FAILED_TO_FIND_HEALTHY_NODES);
    }

    if (healthyNodes.size() < nodesRequired) {
      msg = String.format("Not enough healthy nodes to allocate container. %d "
              + " datanodes required. Found %d",
          nodesRequired, healthyNodes.size());
      LOG.error(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    List<DatanodeDetails> healthyList = healthyNodes.stream().filter(d ->
        hasEnoughSpace(d, sizeRequired)).collect(Collectors.toList());

    if (healthyList.size() < nodesRequired) {
      msg = String.format("Unable to find enough nodes that meet the space " +
              "requirement of %d bytes in healthy node set." +
              " Nodes required: %d Found: %d",
          sizeRequired, nodesRequired, healthyList.size());
      LOG.error(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_NODES_WITH_SPACE);
    }

    return healthyList;
  }

  /**
   * Returns true if this node has enough space to meet our requirement.
   *
   * @param datanodeDetails DatanodeDetails
   * @return true if we have enough space.
   */
  private boolean hasEnoughSpace(DatanodeDetails datanodeDetails,
                                 long sizeRequired) {
    SCMNodeMetric nodeMetric = nodeManager.getNodeStat(datanodeDetails);
    return (nodeMetric != null) && (nodeMetric.get() != null)
        && nodeMetric.get().getRemaining().hasResources(sizeRequired);
  }

  /**
   * This function invokes the derived classes chooseNode Function to build a
   * list of nodes. Then it verifies that invoked policy was able to return
   * expected number of nodes.
   *
   * @param nodesRequired - Nodes Required
   * @param healthyNodes - List of Nodes in the result set.
   * @return List of Datanodes that can be used for placement.
   * @throws SCMException
   */
  public List<DatanodeDetails> getResultSet(
      int nodesRequired, List<DatanodeDetails> healthyNodes)
      throws SCMException {
    List<DatanodeDetails> results = new ArrayList<>();
    for (int x = 0; x < nodesRequired; x++) {
      // invoke the choose function defined in the derived classes.
      DatanodeDetails nodeId = chooseNode(healthyNodes);
      if (nodeId != null) {
        results.add(nodeId);
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
   * Choose a datanode according to the policy, this function is implemented
   * by the actual policy class. For example, PlacementCapacity or
   * PlacementRandom.
   *
   * @param healthyNodes - Set of healthy nodes we can choose from.
   * @return DatanodeDetails
   */
  public abstract DatanodeDetails chooseNode(
      List<DatanodeDetails> healthyNodes);


}
