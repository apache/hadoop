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

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
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
    /**
     * This policy only considers queue length.
     * When allocating, increments queue length without looking at resources
     * available on the node, and when sorting, also only sorts by queue length.
     */
    QUEUE_LENGTH,
    /**
     * This policy only considers the wait time of containers in the queue.
     * Neither looks at resources nor at queue length.
     */
    QUEUE_WAIT_TIME,
    /**
     * This policy considers both queue length and resources.
     * When allocating, first decrements resources available on a node.
     * If resources are available, does not place OContainers on the node queue.
     * When sorting, it first sorts by queue length,
     * then by available resources.
     */
    QUEUE_LENGTH_THEN_RESOURCES;

    private Resource clusterResource = Resources.none();
    private final DominantResourceCalculator resourceCalculator =
        new DominantResourceCalculator();

    private boolean shouldPerformMinRatioComputation() {
      if (clusterResource == null) {
        return false;
      }

      return !resourceCalculator.isAnyMajorResourceZeroOrNegative(
          clusterResource);
    }

    /**
     * Compares queue length of nodes first (shortest first),
     * then compares available resources normalized
     * over cluster resources (most available resources first).
     * @param o1 the first ClusterNode
     * @param o2 the second ClusterNode
     * @return the difference the two ClusterNodes for sorting
     */
    private int compareQueueLengthThenResources(
        final ClusterNode o1, final ClusterNode o2) {
      int diff = o1.getQueueLength() - o2.getQueueLength();
      if (diff != 0) {
        return diff;
      }

      final Resource availableResource1 = o1.getAvailableResource();
      final Resource availableResource2 = o2.getAvailableResource();

      // Cluster resource should be valid before performing min-ratio logic
      // Use raw available resource comparison otherwise
      if (shouldPerformMinRatioComputation()) {
        // Takes the least available resource of the two nodes,
        // normalized to the overall cluster resource
        final float availableRatio1 =
            resourceCalculator.minRatio(availableResource1, clusterResource);
        final float availableRatio2 =
            resourceCalculator.minRatio(availableResource2, clusterResource);

        // The one with more available resources should be placed first
        diff = Precision.compareTo(
            availableRatio2, availableRatio1, Precision.EPSILON);
      }

      if (diff == 0) {
        // Compare absolute value if ratios are the same
        diff = availableResource2.getVirtualCores() - availableResource1.getVirtualCores();
      }

      if (diff == 0) {
        diff = Long.compare(availableResource2.getMemorySize(),
            availableResource1.getMemorySize());
      }

      return diff;
    }

    @Override
    public int compare(ClusterNode o1, ClusterNode o2) {
      int diff;
      switch (this) {
      case QUEUE_LENGTH_THEN_RESOURCES:
        diff = compareQueueLengthThenResources(o1, o2);
        break;
      case QUEUE_WAIT_TIME:
      case QUEUE_LENGTH:
      default:
        diff = getMetric(o1) - getMetric(o2);
        break;
      }

      if (diff == 0) {
        return (int) (o2.getTimestamp() - o1.getTimestamp());
      }
      return diff;
    }

    @VisibleForTesting
    void setClusterResource(Resource clusterResource) {
      this.clusterResource = clusterResource;
    }

    public ResourceCalculator getResourceCalculator() {
      return resourceCalculator;
    }

    public int getMetric(ClusterNode c) {
      switch (this) {
      case QUEUE_WAIT_TIME:
        return c.getQueueWaitTime();
      case QUEUE_LENGTH:
      case QUEUE_LENGTH_THEN_RESOURCES:
      default:
        return c.getQueueLength();
      }
    }

    /**
     * Increment the metric by a delta if it is below the threshold.
     * @param c ClusterNode
     * @param incrementSize increment size
     * @param requested the requested resource
     * @return true if the metric was below threshold and was incremented.
     */
    public boolean compareAndIncrement(
        ClusterNode c, int incrementSize, Resource requested) {
      switch (this) {
      case QUEUE_LENGTH_THEN_RESOURCES:
        return c.compareAndIncrementAllocation(
            incrementSize, resourceCalculator, requested);
      case QUEUE_WAIT_TIME:
        // for queue wait time, we don't have any threshold.
        return true;
      case QUEUE_LENGTH:
      default:
        return c.compareAndIncrementAllocation(incrementSize);
      }
    }

    /**
     * Whether we should be placing OContainers on a node.
     * @param cn the clusterNode
     * @return whether we should be placing OContainers on a node.
     */
    public boolean isNodeAvailable(final ClusterNode cn) {
      int queueCapacity = cn.getQueueCapacity();
      int queueLength = cn.getQueueLength();
      if (this == LoadComparator.QUEUE_LENGTH_THEN_RESOURCES) {
        if (queueCapacity <= 0) {
          return queueLength <= 0;
        } else {
          return queueLength < queueCapacity;
        }
      }
      // In the special case where queueCapacity is 0 for the node,
      // the container can be allocated on the node but will be rejected there
      return queueCapacity <= 0 || queueLength < queueCapacity;
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
            comparator == LoadComparator.QUEUE_LENGTH ||
            comparator == LoadComparator.QUEUE_LENGTH_THEN_RESOURCES)) {
      final ClusterNode.Properties properties =
          ClusterNode.Properties.newInstance()
              .setQueueWaitTime(estimatedQueueWaitTime)
              .setQueueLength(waitQueueLength)
              .setNodeLabels(rmNode.getNodeLabels())
              .setCapability(rmNode.getTotalCapability())
              .setAllocatedResource(rmNode.getAllocatedContainerResource())
              .setQueueCapacity(opportQueueCapacity)
              .updateTimestamp();

      this.clusterNodes.put(rmNode.getNodeID(),
          new ClusterNode(rmNode.getNodeID()).setProperties(properties));

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
            comparator == LoadComparator.QUEUE_LENGTH ||
            comparator == LoadComparator.QUEUE_LENGTH_THEN_RESOURCES)) {
      final ClusterNode.Properties properties =
          ClusterNode.Properties.newInstance()
              .setQueueWaitTime(estimatedQueueWaitTime)
              .setQueueLength(waitQueueLength)
              .setNodeLabels(rmNode.getNodeLabels())
              .setCapability(rmNode.getTotalCapability())
              .setAllocatedResource(rmNode.getAllocatedContainerResource())
              .updateTimestamp();

      clusterNode.setProperties(properties);

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

  /**
   * Selects the node as specified by hostName for resource allocation,
   * unless the node has been blacklisted.
   * @param hostName the hostname of the node for local resource allocation
   * @param blacklist the blacklisted nodes
   * @param request the requested resource
   * @return the selected node, null if the node is full or is blacklisted
   */
  public RMNode selectLocalNode(
      String hostName, Set<String> blacklist, Resource request) {
    if (blacklist.contains(hostName)) {
      return null;
    }
    RMNode node = nodeByHostName.get(hostName);
    if (node != null) {
      ClusterNode clusterNode = clusterNodes.get(node.getNodeID());
      if (clusterNode != null && comparator
          .compareAndIncrement(clusterNode, 1, request)) {
        return node;
      }
    }
    return null;
  }

  /**
   * Selects a node from the rack as specified by rackName
   * for resource allocation, excluding blacklisted nodes
   * @param rackName the rack name for rack-local resource allocation
   * @param blacklist the blacklisted nodes
   * @param request the requested resource
   * @return the selected node, null if no suitable nodes
   */
  public RMNode selectRackLocalNode(
      String rackName, Set<String> blacklist, Resource request) {
    Set<NodeId> nodesOnRack = nodeIdsByRack.get(rackName);
    if (nodesOnRack != null) {
      for (NodeId nodeId : nodesOnRack) {
        if (!blacklist.contains(nodeId.getHost())) {
          ClusterNode node = clusterNodes.get(nodeId);
          if (node != null &&
              comparator.compareAndIncrement(node, 1, request)) {
            return nodeByHostName.get(nodeId.getHost());
          }
        }
      }
    }
    return null;
  }

  /**
   * Selects a node from all ClusterNodes for resource allocation,
   * excluding blacklisted nodes.
   * @param blacklist the blacklisted nodes
   * @param request the requested resource
   * @return the selected node, null if no suitable nodes
   */
  public RMNode selectAnyNode(Set<String> blacklist, Resource request) {
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
        if (node != null && comparator.compareAndIncrement(
            node, 1, request)) {
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
        (k, v) -> {
          v.remove(removedNode.getNodeID());
          return v;
        });
  }

  protected void addIntoNodeIdsByRack(RMNode addedNode) {
    nodeIdsByRack.compute(addedNode.getRackName(), (k, v) -> v == null ?
        ConcurrentHashMap.newKeySet() : v).add(addedNode.getNodeID());
  }

  protected List<ClusterNode> sortNodes(boolean excludeFullNodes) {
    ReentrantReadWriteLock.ReadLock readLock = clusterNodesLock.readLock();
    readLock.lock();
    try {
      final ClusterNode[] nodes = new ClusterNode[clusterNodes.size()];
      int nodesIdx = 0;
      final Resource clusterResource = Resource.newInstance(Resources.none());
      for (final ClusterNode node : this.clusterNodes.values()) {
        Resources.addTo(clusterResource, node.getCapability());
        nodes[nodesIdx] = node;
        nodesIdx++;
      }

      comparator.setClusterResource(clusterResource);

      final List<ClusterNode> retList = new ArrayList<>();
      Arrays.sort(nodes, comparator);
      for (final ClusterNode cNode : nodes) {
        if (!excludeFullNodes || comparator.isNodeAvailable(cNode)) {
          retList.add(cNode);
        }
      }
      return retList;
    } finally {
      readLock.unlock();
    }
  }

}
