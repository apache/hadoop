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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.SCMCommonPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeManager;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container placement policy that randomly choose datanodes with remaining
 * space to satisfy the size constraints.
 * <p>
 * The Algorithm is as follows, Pick 2 random nodes from a given pool of nodes
 * and then pick the node which lower utilization. This leads to a higher
 * probability of nodes with lower utilization to be picked.
 * <p>
 * For those wondering why we choose two nodes randomly and choose the node
 * with lower utilization. There are links to this original papers in
 * HDFS-11564.
 * <p>
 * A brief summary -- We treat the nodes from a scale of lowest utilized to
 * highest utilized, there are (s * ( s + 1)) / 2 possibilities to build
 * distinct pairs of nodes.  There are s - k pairs of nodes in which the rank
 * k node is less than the couple. So probability of a picking a node is
 * (2 * (s -k)) / (s * (s - 1)).
 * <p>
 * In English, There is a much higher probability of picking less utilized nodes
 * as compared to nodes with higher utilization since we pick 2 nodes and
 * then pick the node with lower utilization.
 * <p>
 * This avoids the issue of users adding new nodes into the cluster and HDFS
 * sending all traffic to those nodes if we only use a capacity based
 * allocation scheme. Unless those nodes are part of the set of the first 2
 * nodes then newer nodes will not be in the running to get the container.
 * <p>
 * This leads to an I/O pattern where the lower utilized nodes are favoured
 * more than higher utilized nodes, but part of the I/O will still go to the
 * older higher utilized nodes.
 * <p>
 * With this algorithm in place, our hope is that balancer tool needs to do
 * little or no work and the cluster will achieve a balanced distribution
 * over time.
 */
public final class SCMContainerPlacementCapacity
    extends SCMCommonPlacementPolicy {
  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(SCMContainerPlacementCapacity.class);

  /**
   * Constructs a Container Placement with considering only capacity.
   * That is this policy tries to place containers based on node weight.
   *
   * @param nodeManager Node Manager
   * @param conf Configuration
   */
  public SCMContainerPlacementCapacity(final NodeManager nodeManager,
      final Configuration conf, final NetworkTopology networkTopology,
      final boolean fallback, final SCMContainerPlacementMetrics metrics) {
    super(nodeManager, conf);
  }

  /**
   * Called by SCM to choose datanodes.
   *
   *
   * @param excludedNodes - list of the datanodes to exclude.
   * @param favoredNodes - list of nodes preferred.
   * @param nodesRequired - number of datanodes required.
   * @param sizeRequired - size required for the container or block.
   * @return List of datanodes.
   * @throws SCMException  SCMException
   */
  @Override
  public List<DatanodeDetails> chooseDatanodes(
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes,
      final int nodesRequired, final long sizeRequired) throws SCMException {
    List<DatanodeDetails> healthyNodes = super.chooseDatanodes(excludedNodes,
        favoredNodes, nodesRequired, sizeRequired);
    if (healthyNodes.size() == nodesRequired) {
      return healthyNodes;
    }
    return getResultSet(nodesRequired, healthyNodes);
  }

  /**
   * Find a node from the healthy list and return it after removing it from the
   * list that we are operating on.
   *
   * @param healthyNodes - List of healthy nodes that meet the size
   * requirement.
   * @return DatanodeDetails that is chosen.
   */
  @Override
  public DatanodeDetails chooseNode(List<DatanodeDetails> healthyNodes) {
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
          getNodeManager().getNodeStat(firstNodeDetails);
      SCMNodeMetric secondNodeMetric =
          getNodeManager().getNodeStat(secondNodeDetails);
      datanodeDetails = firstNodeMetric.isGreater(secondNodeMetric.get())
          ? firstNodeDetails : secondNodeDetails;
    }
    healthyNodes.remove(datanodeDetails);
    return datanodeDetails;
  }
}
