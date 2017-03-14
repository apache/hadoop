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

package org.apache.hadoop.ozone.scm.container;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.ozone.scm.node.SCMNodeStat;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.Math.abs;

/**
 * Container placement policy that randomly choose datanodes with remaining
 * space satisfy the size constraints.
 */
public final class SCMContainerPlacementCapacity
    implements ContainerPlacementPolicy {
  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(SCMContainerPlacementCapacity.class);

  private static int maxRetry = 100;
  private final NodeManager nodeManager;
  private final Random rand;
  private final Configuration conf;

  public SCMContainerPlacementCapacity(final NodeManager nodeManager,
      final Configuration conf) {
    this.nodeManager = nodeManager;
    this.rand = new Random();
    this.conf = conf;
  }

  @Override
  public List<DatanodeID> chooseDatanodes(final int nodesRequired,
      final long sizeRequired) throws IOException {

    List<DatanodeID> healthyNodes =
        nodeManager.getNodes(NodeManager.NODESTATE.HEALTHY);

    if (healthyNodes.size() == 0) {
      throw new IOException("No healthy node found to allocate container.");
    }

    if (healthyNodes.size() < nodesRequired) {
      throw new IOException("Not enough nodes to allocate container with " +
          nodesRequired + " datanodes required.");
    }

    if (healthyNodes.size() == nodesRequired) {
      return healthyNodes;
    }

    // TODO: add allocation time as metrics
    long beginTime = Time.monotonicNow();
    Set<DatanodeID> results = new HashSet<>();
    for (int i = 0; i < nodesRequired; i++) {
      DatanodeID candidate = chooseNode(results, healthyNodes, sizeRequired);
      if (candidate != null) {
        results.add(candidate);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding datanode {}. Results.size = {} nodesRequired = {}",
              candidate, results.size(), nodesRequired);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Can't find datanode. Results.size = {} nodesRequired = {}",
              results.size(), nodesRequired);
        }
        break;
      }
    }
    if (LOG.isTraceEnabled()) {
      long endTime = Time.monotonicNow();
      LOG.trace("SCMContainerPlacementCapacity takes {} ms to choose nodes.",
          endTime - beginTime);
    }

    // TODO: handle under replicated case.
    // For now, throw exception only when we can't find any datanode.
    if (results.size() == 0) {
      throw new IOException("No healthy node found " +
          "with enough remaining capacity to allocate container.");
    }

    if (results.size() != nodesRequired) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("SCMContainerPlacementCapacity cannot find enough healthy" +
                " datanodes with remaining capacity > {} ." +
                "(nodesRequired = {}, nodesFound = {})", sizeRequired,
            nodesRequired, results.size());
      }
    }

    return results.stream().collect(Collectors.toList());
  }

  /**
   * Choose one random node from 2-Random nodes that satisfy the size required.
   * @param results - set of current chosen datanodes.
   * @param healthyNodes - all healthy datanodes.
   * @param sizeRequired - size required for container.
   * @return one with larger remaining capacity from two randomly chosen
   *         datanodes that satisfy sizeRequirement but are not in current
   *         result set.
   */
  private DatanodeID chooseNode(final Set results,
      final List<DatanodeID> healthyNodes, final long sizeRequired) {
    NodeAndStat firstNode = chooseOneNode(results, healthyNodes,
        sizeRequired);
    if (firstNode == null) {
      return null;
    }

    NodeAndStat secondNode = chooseOneNode(results, healthyNodes,
        sizeRequired);
    if (secondNode == null) {
      return firstNode.getDatanodeID();
    }

    // Pick one with larger remaining space.
    return firstNode.getDatanodeStat().getRemaining() >
        secondNode.getDatanodeStat().getRemaining() ?
        firstNode.getDatanodeID() : secondNode.getDatanodeID();
  }

  /**
   * Choose one random node from healthy nodes that satisfies the size
   * requirement and has not been chosen in the existing results.
   * Retry up to maxRetry(100) times.
   * @param results - set of current chosen datanodes.
   * @param healthyNodes - all healthy datanodes.
   * @param sizeRequired - size required for container.
   * @return one with larger remaining capacity from two randomly chosen
   *         datanodes that satisfy sizeRequirement but are not in current
   *         result set.
   */
  private NodeAndStat chooseOneNode(final Set<DatanodeID> results,
      final List<DatanodeID> healthyNodes, final long sizeRequired) {
    NodeAndStat selectedNode = null;
    int retry = 0;
    while (selectedNode == null && retry < maxRetry) {
      int candidateIdx = abs(rand.nextInt() % healthyNodes.size());
      DatanodeID candidate = healthyNodes.get(candidateIdx);
      if (!results.contains(candidate)) {
        SCMNodeStat stat = nodeManager.getNodeStat(candidate);
        if (stat != null && stat.getRemaining() > sizeRequired) {
          selectedNode = new NodeAndStat(candidate, stat);
          break;
        }
      }
      retry++;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Find {} after {} retries!", (selectedNode != null) ?
              selectedNode.getDatanodeID() : "no datanode", retry);
    }
    return selectedNode;
  }

  /**
   * Helper class wraps DatanodeID and SCMNodeStat.
   */
  static class NodeAndStat {
    private final DatanodeID datanodeID;
    private final SCMNodeStat stat;

    NodeAndStat(final DatanodeID id, final SCMNodeStat stat) {
      this.datanodeID = id;
      this.stat = stat;
    }

    public DatanodeID getDatanodeID() {
      return datanodeID;
    }

    public SCMNodeStat getDatanodeStat() {
      return stat;
    }
  }
}
