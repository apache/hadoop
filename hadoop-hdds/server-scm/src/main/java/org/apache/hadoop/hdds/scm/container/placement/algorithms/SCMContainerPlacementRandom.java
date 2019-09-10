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
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.SCMCommonPlacementPolicy;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Container placement policy that randomly chooses healthy datanodes.
 * This is very similar to current HDFS placement. That is we
 * just randomly place containers without any considerations of utilization.
 * <p>
 * That means we rely on balancer to achieve even distribution of data.
 * Balancer will need to support containers as a feature before this class
 * can be practically used.
 */
public final class SCMContainerPlacementRandom extends SCMCommonPlacementPolicy
    implements PlacementPolicy {
  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(SCMContainerPlacementRandom.class);

  /**
   * Construct a random Block Placement policy.
   *
   * @param nodeManager nodeManager
   * @param conf Config
   */
  public SCMContainerPlacementRandom(final NodeManager nodeManager,
      final Configuration conf, final NetworkTopology networkTopology,
      final boolean fallback, final SCMContainerPlacementMetrics metrics) {
    super(nodeManager, conf);
  }

  /**
   * Choose datanodes called by the SCM to choose the datanode.
   *
   *
   * @param excludedNodes - list of the datanodes to exclude.
   * @param favoredNodes - list of nodes preferred.
   * @param nodesRequired - number of datanodes required.
   * @param sizeRequired - size required for the container or block.
   * @return List of Datanodes.
   * @throws SCMException  SCMException
   */
  @Override
  public List<DatanodeDetails> chooseDatanodes(
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes,
      final int nodesRequired, final long sizeRequired) throws SCMException {
    List<DatanodeDetails> healthyNodes =
        super.chooseDatanodes(excludedNodes, favoredNodes, nodesRequired,
            sizeRequired);

    if (healthyNodes.size() == nodesRequired) {
      return healthyNodes;
    }
    return getResultSet(nodesRequired, healthyNodes);
  }

  /**
   * Just chose a node randomly and remove it from the set of nodes we can
   * chose from.
   *
   * @param healthyNodes - all healthy datanodes.
   * @return one randomly chosen datanode that from two randomly chosen datanode
   */
  @Override
  public DatanodeDetails chooseNode(final List<DatanodeDetails> healthyNodes) {
    DatanodeDetails selectedNode =
        healthyNodes.get(getRand().nextInt(healthyNodes.size()));
    healthyNodes.remove(selectedNode);
    return selectedNode;
  }
}
