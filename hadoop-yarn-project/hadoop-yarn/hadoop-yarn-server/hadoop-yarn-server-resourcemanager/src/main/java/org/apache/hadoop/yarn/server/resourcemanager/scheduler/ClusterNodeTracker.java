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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private static final Log LOG = LogFactory.getLog(ClusterNodeTracker.class);

  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
  private Lock readLock = readWriteLock.readLock();
  private Lock writeLock = readWriteLock.writeLock();

  private HashMap<NodeId, N> nodes = new HashMap<>();
  private Map<String, Integer> nodesPerRack = new HashMap<>();

  private Resource clusterCapacity = Resources.clone(Resources.none());
  private Resource staleClusterCapacity = null;

  // Max allocation
  private long maxNodeMemory = -1;
  private long maxNodeVCores = -1;
  private Resource configuredMaxAllocation;
  private boolean forceConfiguredMaxAllocation = true;
  private long configuredMaxAllocationWaitTime;

  public void addNode(N node) {
    writeLock.lock();
    try {
      nodes.put(node.getNodeID(), node);

      // Update nodes per rack as well
      String rackName = node.getRackName();
      Integer numNodes = nodesPerRack.get(rackName);
      if (numNodes == null) {
        numNodes = 0;
      }
      nodesPerRack.put(rackName, ++numNodes);

      // Update cluster capacity
      Resources.addTo(clusterCapacity, node.getTotalResource());

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
      Integer nodeCount = nodesPerRack.get(rName);
      return nodeCount == null ? 0 : nodeCount;
    } finally {
      readLock.unlock();
    }
  }

  public Resource getClusterCapacity() {
    readLock.lock();
    try {
      if (staleClusterCapacity == null ||
          !Resources.equals(staleClusterCapacity, clusterCapacity)) {
        staleClusterCapacity = Resources.clone(clusterCapacity);
      }
      return staleClusterCapacity;
    } finally {
      readLock.unlock();
    }
  }

  public N removeNode(NodeId nodeId) {
    writeLock.lock();
    try {
      N node = nodes.remove(nodeId);
      if (node == null) {
        LOG.warn("Attempting to remove a non-existent node " + nodeId);
        return null;
      }

      // Update nodes per rack as well
      String rackName = node.getRackName();
      Integer numNodes = nodesPerRack.get(rackName);
      if (numNodes > 0) {
        nodesPerRack.put(rackName, --numNodes);
      } else {
        LOG.error("Attempting to remove node from an empty rack " + rackName);
      }

      // Update cluster capacity
      Resources.subtractFrom(clusterCapacity, node.getTotalResource());

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

      if (forceConfiguredMaxAllocation
          || maxNodeMemory == -1 || maxNodeVCores == -1) {
        return configuredMaxAllocation;
      }

      return Resources.createResource(
          Math.min(configuredMaxAllocation.getMemorySize(), maxNodeMemory),
          Math.min(configuredMaxAllocation.getVirtualCores(), maxNodeVCores)
      );
    } finally {
      readLock.unlock();
    }
  }

  private void updateMaxResources(SchedulerNode node, boolean add) {
    Resource totalResource = node.getTotalResource();
    writeLock.lock();
    try {
      if (add) { // added node
        long nodeMemory = totalResource.getMemorySize();
        if (nodeMemory > maxNodeMemory) {
          maxNodeMemory = nodeMemory;
        }
        int nodeVCores = totalResource.getVirtualCores();
        if (nodeVCores > maxNodeVCores) {
          maxNodeVCores = nodeVCores;
        }
      } else {  // removed node
        if (maxNodeMemory == totalResource.getMemorySize()) {
          maxNodeMemory = -1;
        }
        if (maxNodeVCores == totalResource.getVirtualCores()) {
          maxNodeVCores = -1;
        }
        // We only have to iterate through the nodes if the current max memory
        // or vcores was equal to the removed node's
        if (maxNodeMemory == -1 || maxNodeVCores == -1) {
          // Treat it like an empty cluster and add nodes
          for (N n : nodes.values()) {
            updateMaxResources(n, true);
          }
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

  /**
   * Convenience method to sort nodes.
   *
   * Note that the sort is performed without holding a lock. We are sorting
   * here instead of on the caller to allow for future optimizations (e.g.
   * sort once every x milliseconds).
   */
  public List<N> sortedNodeList(Comparator<N> comparator) {
    List<N> sortedList = null;
    readLock.lock();
    try {
      sortedList = new ArrayList(nodes.values());
    } finally {
      readLock.unlock();
    }
    Collections.sort(sortedList, comparator);
    return sortedList;
  }
}