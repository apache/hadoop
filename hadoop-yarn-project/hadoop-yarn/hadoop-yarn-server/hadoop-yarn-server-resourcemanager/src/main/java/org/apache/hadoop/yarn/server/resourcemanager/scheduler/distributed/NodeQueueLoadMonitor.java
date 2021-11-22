/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_OPP_CONTAINER_ALLOCATION_NODES_NUMBER_USED;

/**
 * The NodeQueueLoadMonitor keeps track of load metrics (such as queue length
 * and total wait time) associated with Container Queues on the Node Manager.
 * It uses this information to periodically sort the Nodes from least to most
 * loaded.
 */
public class NodeQueueLoadMonitor implements ClusterMonitor {

  protected final static Logger LOG = LoggerFactory.
      getLogger(NodeQueueLoadMonitor.class);

  protected int numNodesForAnyAllocation =
      DEFAULT_OPP_CONTAINER_ALLOCATION_NODES_NUMBER_USED;

  /**
   * The comparator used to specify the metric against which the load
   * of two Nodes are compared.
   */
  public enum LoadComparator implements Comparator<ClusterNode> {
    QUEUE_LENGTH,
    QUEUE_WAIT_TIME;

    @Override
    public int compare(ClusterNode o1, ClusterNode o2) {
      if (getMetric(o1) == getMetric(o2)) {
        return (int)(o2.getTimestamp() - o1.getTimestamp());
      }
      return getMetric(o1) - getMetric(o2);
    }

    public int getMetric(ClusterNode c) {
      return (this == QUEUE_LENGTH) ?
          c.getQueueLength().get() : c.getQueueWaitTime().get();
    }

    /**
     * Increment the metric by a delta if it is below the threshold.
     * @param c ClusterNode
     * @param incrementSize increment size
     * @return true if the metric was below threshold and was incremented.
     */
    public boolean compareAndIncrement(ClusterNode c, int incrementSize) {
      if(this == QUEUE_LENGTH) {
        int ret = c.getQueueLength().addAndGet(incrementSize);
        if (ret <= c.getQueueCapacity()) {
          return true;
        }
        c.getQueueLength().addAndGet(-incrementSize);
        return false;
      }
      // for queue wait time, we don't have any threshold.
      return true;
    }
  }

  private final ScheduledExecutorService scheduledExecutor;

  protected final List<NodeId> sortedNodes;
  protected final Map<NodeId, ClusterNode> clusterNodes =
      new ConcurrentHashMap<>();
  protected final Map<String, RMNode> nodeByHostName =
      new ConcurrentHashMap<>();
  protected final Map<String, Set<NodeId>> nodeIdsByRack =
      new ConcurrentHashMap<>();
  protected final LoadComparator comparator;
  protected QueueLimitCalculator thresholdCalculator;
  protected ReentrantReadWriteLock sortedNodesLock = new ReentrantReadWriteLock();
  protected ReentrantReadWriteLock clusterNodesLock =
      new ReentrantReadWriteLock();

  Runnable computeTask = new Runnable() {
    @Override
    public void run() {
      ReentrantReadWriteLock.WriteLock writeLock = sortedNodesLock.writeLock();
      writeLock.lock();
      try {
        try {
          updateSortedNodes();
        } catch (Exception ex) {
          LOG.warn("Got Exception while sorting nodes..", ex);
        }
        if (thresholdCalculator != null) {
          thresholdCalculator.update();
        }
      } finally {
        writeLock.unlock();
      }
    }
  };

  @VisibleForTesting
  NodeQueueLoadMonitor(LoadComparator comparator) {
    this.sortedNodes = new ArrayList<>();
    this.comparator = comparator;
    this.scheduledExecutor = null;
  }

  public NodeQueueLoadMonitor(long nodeComputationInterval,
      LoadComparator comparator, int numNodes) {
    this.sortedNodes = new ArrayList<>();
    this.scheduledExecutor = Executors.newScheduledThreadPool(1);
    this.comparator = comparator;
    this.scheduledExecutor.scheduleAtFixedRate(computeTask,
        nodeComputationInterval, nodeComputationInterval,
        TimeUnit.MILLISECONDS);
    numNodesForAnyAllocation = numNodes;
  }

  protected void updateSortedNodes() {
    List<NodeId> nodeIds = sortNodes(true).stream()
        .map(n -> n.nodeId)
        .collect(Collectors.toList());
    sortedNodes.clear();
    sortedNodes.addAll(nodeIds);
  }

  List<NodeId> getSortedNodes() {
    return sortedNodes;
  }

  public QueueLimitCalculator getThresholdCalculator() {
    return thresholdCalculator;
  }

  public void stop() {
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
    }
  }

  Map<NodeId, ClusterNode> getClusterNodes() {
    return clusterNodes;
  }

  Comparator<ClusterNode> getComparator() {
    return comparator;
  }

  public void initThresholdCalculator(float sigma, int limitMin, int limitMax) {
    this.thresholdCalculator =
        new QueueLimitCalculator(this, sigma, limitMin, limitMax);
  }

  @Override
  public void addNode(List<NMContainerStatus> containerStatuses,
      RMNode rmNode) {
    this.nodeByHostName.put(rmNode.getHostName(), rmNode);
    addIntoNodeIdsByRack(rmNode);
    // Ignoring this currently : at least one NODE_UPDATE heartbeat is
    // required to ensure node eligibility.
  }

  @Override
  public void removeNode(RMNode removedRMNode) {
    LOG.info("Node delete event for: {}", removedRMNode.getNode().getName());
    this.nodeByHostName.remove(removedRMNode.getHostName());
    removeFromNodeIdsByRack(removedRMNode);
    ReentrantReadWriteLock.WriteLock writeLock = clusterNodesLock.writeLock();
    writeLock.lock();
    ClusterNode node;
    try {
      node = this.clusterNodes.remove(removedRMNode.getNodeID());
      onNodeRemoved(node);
    } finally {
      writeLock.unlock();
    }
    if (LOG.isDebugEnabled()) {
      if (node != null) {
        LOG.debug("Delete ClusterNode: " + removedRMNode.getNodeID());
      } else {
        LOG.debug("Node not in list!");
      }
    }
  }

  /**
   * Provide an integration point for extended class
   * @param node the node removed
   */
  protected void onNodeRemoved(ClusterNode node) {
  }

  @Override
  public void updateNode(RMNode rmNode) {
    LOG.debug("Node update event from: {}", rmNode.getNodeID());
    OpportunisticContainersStatus opportunisticContainersStatus =
        rmNode.getOpportunisticContainersStatus();
    if (opportunisticContainersStatus == null) {
      opportunisticContainersStatus =
          OpportunisticContainersStatus.newInstance();
    }

    // Add nodes to clusterNodes. If estimatedQueueTime is -1, ignore node
    // UNLESS comparator is based on queue length.
    ReentrantReadWriteLock.WriteLock writeLock = clusterNodesLock.writeLock();
    writeLock.lock();
    try {
      ClusterNode clusterNode = this.clusterNodes.get(rmNode.getNodeID());
      if (clusterNode == null) {
        onNewNodeAdded(rmNode, opportunisticContainersStatus);
      } else {
        onExistingNodeUpdated(rmNode, clusterNode, opportunisticContainersStatus);
      }
    } finally {
      writeLock.unlock();
    }
  }

  protected void onNewNodeAdded(
      RMNode rmNode, OpportunisticContainersStatus status) {
    int opportQueueCapacity = status.getOpportQueueCapacity();
    int estimatedQueueWaitTime = status.getEstimatedQueueWaitTime();
    int waitQueueLength = status.getWaitQueueLength();

    if (rmNode.getState() != NodeState.DECOMMISSIONING &&
        (estimatedQueueWaitTime != -1 ||
            comparator == LoadComparator.QUEUE_LENGTH)) {
      this.clusterNodes.put(rmNode.getNodeID(),
          new ClusterNode(rmNode.getNodeID())
              .setQueueWaitTime(estimatedQueueWaitTime)
              .setQueueLength(waitQueueLength)
              .setNodeLabels(rmNode.getNodeLabels())
              .setQueueCapacity(opportQueueCapacity));
      LOG.info(
          "Inserting ClusterNode [{}] with queue wait time [{}] and "
              + "wait queue length [{}]",
          rmNode.getNode(),
          estimatedQueueWaitTime,
          waitQueueLength
      );
    } else {
      LOG.warn(
          "IGNORING ClusterNode [{}] with queue wait time [{}] and "
              + "wait queue length [{}]",
          rmNode.getNode(),
          estimatedQueueWaitTime,
          waitQueueLength
      );
    }
  }

  protected void onExistingNodeUpdated(
      RMNode rmNode, ClusterNode clusterNode,
      OpportunisticContainersStatus status) {

    int estimatedQueueWaitTime = status.getEstimatedQueueWaitTime();
    int waitQueueLength = status.getWaitQueueLength();

    if (rmNode.getState() != NodeState.DECOMMISSIONING &&
        (estimatedQueueWaitTime != -1 ||
            comparator == LoadComparator.QUEUE_LENGTH)) {
      clusterNode
          .setQueueWaitTime(estimatedQueueWaitTime)
          .setQueueLength(waitQueueLength)
          .setNodeLabels(rmNode.getNodeLabels())
          .updateTimestamp();
      LOG.debug("Updating ClusterNode [{}] with queue wait time [{}] and"
              + " wait queue length [{}]", rmNode.getNodeID(),
          estimatedQueueWaitTime, waitQueueLength);

    } else {
      this.clusterNodes.remove(rmNode.getNodeID());
      LOG.info("Deleting ClusterNode [" + rmNode.getNodeID() + "] " +
          "with queue wait time [" + clusterNode.getQueueWaitTime() + "] and " +
          "wait queue length [" + clusterNode.getQueueLength() + "]");
    }
  }

  @Override
  public void updateNodeResource(RMNode rmNode, ResourceOption resourceOption) {
    LOG.debug("Node resource update event from: {}", rmNode.getNodeID());
    // Ignoring this currently.
  }

  /**
   * Returns all Node Ids as ordered list from Least to Most Loaded.
   * @return ordered list of nodes
   */
  public List<NodeId> selectNodes() {
    return selectLeastLoadedNodes(-1);
  }

  /**
   * Returns 'K' of the least Loaded Node Ids as ordered list.
   * @param k max number of nodes to return
   * @return ordered list of nodes
   */
  public List<NodeId> selectLeastLoadedNodes(int k) {
    ReentrantReadWriteLock.ReadLock readLock = sortedNodesLock.readLock();
    readLock.lock();
    try {
      List<NodeId> retVal = ((k < this.sortedNodes.size()) && (k >= 0)) ?
          new ArrayList<>(this.sortedNodes).subList(0, k) :
          new ArrayList<>(this.sortedNodes);
      return retVal;
    } finally {
      readLock.unlock();
    }
  }

  public RMNode selectLocalNode(String hostName, Set<String> blacklist) {
    if (blacklist.contains(hostName)) {
      return null;
    }
    RMNode node = nodeByHostName.get(hostName);
    if (node != null) {
      ClusterNode clusterNode = clusterNodes.get(node.getNodeID());
      if (comparator.compareAndIncrement(clusterNode, 1)) {
        return node;
      }
    }
    return null;
  }

  public RMNode selectRackLocalNode(String rackName, Set<String> blacklist) {
    Set<NodeId> nodesOnRack = nodeIdsByRack.get(rackName);
    if (nodesOnRack != null) {
      for (NodeId nodeId : nodesOnRack) {
        if (!blacklist.contains(nodeId.getHost())) {
          ClusterNode node = clusterNodes.get(nodeId);
          if (node != null && comparator.compareAndIncrement(node, 1)) {
            return nodeByHostName.get(nodeId.getHost());
          }
        }
      }
    }
    return null;
  }

  public RMNode selectAnyNode(Set<String> blacklist) {
    List<NodeId> nodeIds = getCandidatesForSelectAnyNode();
    int size = nodeIds.size();
    if (size <= 0) {
      return null;
    }
    Random rand = new Random();
    int startIndex = rand.nextInt(size);
    for (int i = 0; i < size; ++i) {
      int index = i + startIndex;
      index %= size;
      NodeId nodeId = nodeIds.get(index);
      if (nodeId != null && !blacklist.contains(nodeId.getHost())) {
        ClusterNode node = clusterNodes.get(nodeId);
        if (node != null && comparator.compareAndIncrement(node, 1)) {
          return nodeByHostName.get(nodeId.getHost());
        }
      }
    }
    return null;
  }

  protected List<NodeId> getCandidatesForSelectAnyNode() {
    return selectLeastLoadedNodes(numNodesForAnyAllocation);
  }

  protected void removeFromNodeIdsByRack(RMNode removedNode) {
    nodeIdsByRack.computeIfPresent(removedNode.getRackName(),
        (k, v) -> v).remove(removedNode.getNodeID());
  }

  protected void addIntoNodeIdsByRack(RMNode addedNode) {
    nodeIdsByRack.compute(addedNode.getRackName(), (k, v) -> v == null ?
        ConcurrentHashMap.newKeySet() : v).add(addedNode.getNodeID());
  }

  protected List<ClusterNode> sortNodes(boolean excludeFullNodes) {
    ReentrantReadWriteLock.ReadLock readLock = clusterNodesLock.readLock();
    readLock.lock();
    try {
      ArrayList<ClusterNode> aList = new ArrayList<>(this.clusterNodes.values());
      List<ClusterNode> retList = new ArrayList<>();
      Object[] nodes = aList.toArray();
      // Collections.sort would do something similar by calling Arrays.sort
      // internally but would finally iterate through the input list (aList)
      // to reset the value of each element. Since we don't really care about
      // 'aList', we can use the iteration to create the list of nodeIds which
      // is what we ultimately care about.
      Arrays.sort(nodes, (Comparator)comparator);
      for (int j=0; j < nodes.length; j++) {
        ClusterNode cNode = (ClusterNode)nodes[j];
        // Only add node to the result list when either condition is met:
        // 1. we don't exclude full nodes
        // 2. we do exclude full nodes, but the current node is not full
        if (!excludeFullNodes || !cNode.isQueueFull()) {
          retList.add(cNode);
        }
      }
      return retList;
    } finally {
      readLock.unlock();
    }
  }

}
