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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Helper library that:
 * - tracks the state of all cluster {@link SchedulerNode}s
 * - provides convenience methods to filter and sort nodes
 */
@InterfaceAudience.Private
public class ClusterNodeTracker<N extends SchedulerNode> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ClusterNodeTracker.class);

  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
  private Lock readLock = readWriteLock.readLock();
  private Lock writeLock = readWriteLock.writeLock();

  private HashMap<NodeId, N> nodes = new HashMap<>();
  private Map<String, N> nodeNameToNodeMap = new HashMap<>();
  private Map<String, List<N>> nodesPerRack = new HashMap<>();
  private Map<String, List<N>> nodesPerLabel = new HashMap<>();

  private Resource clusterCapacity = Resources.createResource(0, 0);
  private volatile Resource staleClusterCapacity =
      Resources.clone(Resources.none());

  // Max allocation
  private final long[] maxAllocation;
  private Resource configuredMaxAllocation;
  private boolean forceConfiguredMaxAllocation = true;
  private long configuredMaxAllocationWaitTime;
  private boolean reportedMaxAllocation = false;

  public ClusterNodeTracker() {
    maxAllocation = new long[ResourceUtils.getNumberOfCountableResourceTypes()];
    Arrays.fill(maxAllocation, -1);
  }

  public void addNode(N node) {
    writeLock.lock();
    try {
      nodes.put(node.getNodeID(), node);
      nodeNameToNodeMap.put(node.getNodeName(), node);

      List<N> nodesPerLabels = nodesPerLabel.get(node.getPartition());

      if (nodesPerLabels == null) {
        nodesPerLabels = new ArrayList<N>();
      }
      nodesPerLabels.add(node);

      // Update new set of nodes for given partition.
      nodesPerLabel.put(node.getPartition(), nodesPerLabels);

      // Update nodes per rack as well
      String rackName = node.getRackName();
      List<N> nodesList = nodesPerRack.get(rackName);
      if (nodesList == null) {
        nodesList = new ArrayList<>();
        nodesPerRack.put(rackName, nodesList);
      }
      nodesList.add(node);

      // Update cluster capacity
      Resources.addTo(clusterCapacity, node.getTotalResource());
      staleClusterCapacity = Resources.clone(clusterCapacity);
      ClusterMetrics.getMetrics().incrCapability(node.getTotalResource());

      // Update maximumAllocation
      updateMaxResources(node, true);
    } finally {
      writeLock.unlock();
    }
  }

  public boolean exists(NodeId nodeId) {
    readLock.lock();
    try {
      return nodes.containsKey(nodeId);
    } finally {
      readLock.unlock();
    }
  }

  public N getNode(NodeId nodeId) {
    readLock.lock();
    try {
      return nodes.get(nodeId);
    } finally {
      readLock.unlock();
    }
  }

  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    readLock.lock();
    try {
      N n = nodes.get(nodeId);
      return n == null ? null : new SchedulerNodeReport(n);
    } finally {
      readLock.unlock();
    }
  }

  public int nodeCount() {
    readLock.lock();
    try {
      return nodes.size();
    } finally {
      readLock.unlock();
    }
  }

  public int nodeCount(String rackName) {
    readLock.lock();
    String rName = rackName == null ? "NULL" : rackName;
    try {
      List<N> nodesList = nodesPerRack.get(rName);
      return nodesList == null ? 0 : nodesList.size();
    } finally {
      readLock.unlock();
    }
  }

  public Resource getClusterCapacity() {
    return staleClusterCapacity;
  }

  public N removeNode(NodeId nodeId) {
    writeLock.lock();
    try {
      N node = nodes.remove(nodeId);
      if (node == null) {
        LOG.warn("Attempting to remove a non-existent node " + nodeId);
        return null;
      }
      nodeNameToNodeMap.remove(node.getNodeName());

      // Update nodes per rack as well
      String rackName = node.getRackName();
      List<N> nodesList = nodesPerRack.get(rackName);
      if (nodesList == null) {
        LOG.error("Attempting to remove node from an empty rack " + rackName);
      } else {
        nodesList.remove(node);
        if (nodesList.isEmpty()) {
          nodesPerRack.remove(rackName);
        }
      }

      List<N> nodesPerPartition = nodesPerLabel.get(node.getPartition());
      nodesPerPartition.remove(node);

      // Update new set of nodes for given partition.
      if (nodesPerPartition.isEmpty()) {
        nodesPerLabel.remove(node.getPartition());
      } else {
        nodesPerLabel.put(node.getPartition(), nodesPerPartition);
      }

      // Update cluster capacity
      Resources.subtractFrom(clusterCapacity, node.getTotalResource());
      staleClusterCapacity = Resources.clone(clusterCapacity);
      ClusterMetrics.getMetrics().decrCapability(node.getTotalResource());

      // Update maximumAllocation
      updateMaxResources(node, false);

      return node;
    } finally {
      writeLock.unlock();
    }
  }

  public void setConfiguredMaxAllocation(Resource resource) {
    writeLock.lock();
    try {
      configuredMaxAllocation = Resources.clone(resource);
    } finally {
      writeLock.unlock();
    }
  }

  public void setConfiguredMaxAllocationWaitTime(
      long configuredMaxAllocationWaitTime) {
    writeLock.lock();
    try {
      this.configuredMaxAllocationWaitTime =
          configuredMaxAllocationWaitTime;
    } finally {
      writeLock.unlock();
    }
  }

  public Resource getMaxAllowedAllocation() {
    readLock.lock();
    try {
      if (forceConfiguredMaxAllocation &&
          System.currentTimeMillis() - ResourceManager.getClusterTimeStamp()
              > configuredMaxAllocationWaitTime) {
        forceConfiguredMaxAllocation = false;
      }

      if (forceConfiguredMaxAllocation || !reportedMaxAllocation) {
        return configuredMaxAllocation;
      }

      Resource ret = Resources.clone(configuredMaxAllocation);

      for (int i = 0; i < maxAllocation.length; i++) {
        ResourceInformation info = ret.getResourceInformation(i);

        if (info.getValue() > maxAllocation[i]) {
          info.setValue(maxAllocation[i]);
        }
      }

      return ret;
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  public void setForceConfiguredMaxAllocation(boolean flag) {
    writeLock.lock();
    try {
      forceConfiguredMaxAllocation = flag;
    } finally {
      writeLock.unlock();
    }
  }

  private void updateMaxResources(SchedulerNode node, boolean add) {
    Resource totalResource = node.getTotalResource();
    ResourceInformation[] totalResources;

    if (totalResource != null) {
      totalResources = totalResource.getResources();
    } else {
      LOG.warn(node.getNodeName() + " reported in with null resources, which "
          + "indicates a problem in the source code. Please file an issue at "
          + "https://issues.apache.org/jira/secure/CreateIssue!default.jspa");

      return;
    }

    writeLock.lock();

    try {
      if (add) { // added node
        // If we add a node, we must have a max allocation for all resource
        // types
        reportedMaxAllocation = true;

        for (int i = 0; i < maxAllocation.length; i++) {
          long value = totalResources[i].getValue();

          if (value > maxAllocation[i]) {
            maxAllocation[i] = value;
          }
        }
      } else {  // removed node
        boolean recalculate = false;

        for (int i = 0; i < maxAllocation.length; i++) {
          if (totalResources[i].getValue() == maxAllocation[i]) {
            // No need to set reportedMaxAllocation to false here because we
            // will recalculate before we release the lock.
            maxAllocation[i] = -1;
            recalculate = true;
          }
        }

        // We only have to iterate through the nodes if the current max memory
        // or vcores was equal to the removed node's
        if (recalculate) {
          // Treat it like an empty cluster and add nodes
          reportedMaxAllocation = false;
          nodes.values().forEach(n -> updateMaxResources(n, true));
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  public List<N> getAllNodes() {
    return getNodes(null);
  }

  /**
   * Convenience method to filter nodes based on a condition.
   *
   * @param nodeFilter A {@link NodeFilter} for filtering the nodes
   * @return A list of filtered nodes
   */
  public List<N> getNodes(NodeFilter nodeFilter) {
    List<N> nodeList = new ArrayList<>();
    readLock.lock();
    try {
      if (nodeFilter == null) {
        nodeList.addAll(nodes.values());
      } else {
        for (N node : nodes.values()) {
          if (nodeFilter.accept(node)) {
            nodeList.add(node);
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return nodeList;
  }

  public List<NodeId> getAllNodeIds() {
    return getNodeIds(null);
  }

  /**
   * Convenience method to filter nodes based on a condition.
   *
   * @param nodeFilter A {@link NodeFilter} for filtering the nodes
   * @return A list of filtered nodes
   */
  public List<NodeId> getNodeIds(NodeFilter nodeFilter) {
    List<NodeId> nodeList = new ArrayList<>();
    readLock.lock();
    try {
      if (nodeFilter == null) {
        for (N node : nodes.values()) {
          nodeList.add(node.getNodeID());
        }
      } else {
        for (N node : nodes.values()) {
          if (nodeFilter.accept(node)) {
            nodeList.add(node.getNodeID());
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return nodeList;
  }

  /**
   * Convenience method to sort nodes.
   * Nodes can change while being sorted. Using a standard sort will fail
   * without locking each node, the TreeSet handles this without locks.
   *
   * @param comparator the comparator to sort the nodes with
   * @return sorted set of nodes in the form of a TreeSet
   */
  public TreeSet<N> sortedNodeSet(Comparator<N> comparator) {
    TreeSet<N> sortedSet = new TreeSet<>(comparator);
    readLock.lock();
    try {
      sortedSet.addAll(nodes.values());
    } finally {
      readLock.unlock();
    }
    return sortedSet;
  }

  /**
   * Convenience method to return list of nodes corresponding to resourceName
   * passed in the {@link ResourceRequest}.
   *
   * @param resourceName Host/rack name of the resource, or
   * {@link ResourceRequest#ANY}
   * @return list of nodes that match the resourceName
   */
  public List<N> getNodesByResourceName(final String resourceName) {
    Preconditions.checkArgument(
        resourceName != null && !resourceName.isEmpty());
    List<N> retNodes = new ArrayList<>();
    if (ResourceRequest.ANY.equals(resourceName)) {
      retNodes.addAll(getAllNodes());
    } else if (nodeNameToNodeMap.containsKey(resourceName)) {
      retNodes.add(nodeNameToNodeMap.get(resourceName));
    } else if (nodesPerRack.containsKey(resourceName)) {
      retNodes.addAll(nodesPerRack.get(resourceName));
    } else {
      LOG.info(
          "Could not find a node matching given resourceName " + resourceName);
    }
    return retNodes;
  }

  /**
   * Convenience method to return list of {@link NodeId} corresponding to
   * resourceName passed in the {@link ResourceRequest}.
   *
   * @param resourceName Host/rack name of the resource, or
   * {@link ResourceRequest#ANY}
   * @return list of {@link NodeId} that match the resourceName
   */
  public List<NodeId> getNodeIdsByResourceName(final String resourceName) {
    Preconditions.checkArgument(
        resourceName != null && !resourceName.isEmpty());
    List<NodeId> retNodes = new ArrayList<>();
    if (ResourceRequest.ANY.equals(resourceName)) {
      retNodes.addAll(getAllNodeIds());
    } else if (nodeNameToNodeMap.containsKey(resourceName)) {
      retNodes.add(nodeNameToNodeMap.get(resourceName).getNodeID());
    } else if (nodesPerRack.containsKey(resourceName)) {
      for (N node : nodesPerRack.get(resourceName)) {
        retNodes.add(node.getNodeID());
      }
    } else {
      LOG.info(
          "Could not find a node matching given resourceName " + resourceName);
    }
    return retNodes;
  }

  /**
   * update cached nodes per partition on a node label change event.
   * @param partition nodeLabel
   * @param nodeIds List of Node IDs
   */
  public void updateNodesPerPartition(String partition, Set<NodeId> nodeIds) {
    writeLock.lock();
    try {
      // Clear all entries.
      nodesPerLabel.remove(partition);

      List<N> nodesPerPartition = new ArrayList<N>();
      for (NodeId nodeId : nodeIds) {
        N n = getNode(nodeId);
        if (n != null) {
          nodesPerPartition.add(n);
        }
      }

      // Update new set of nodes for given partition.
      nodesPerLabel.put(partition, nodesPerPartition);
    } finally {
      writeLock.unlock();
    }
  }

  public List<N> getNodesPerPartition(String partition) {
    List<N> nodesPerPartition = null;
    readLock.lock();
    try {
      if (nodesPerLabel.containsKey(partition)) {
        nodesPerPartition = new ArrayList<N>(nodesPerLabel.get(partition));
      }
    } finally {
      readLock.unlock();
    }
    return nodesPerPartition;
  }
}
