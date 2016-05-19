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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.records.QueuedContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The NodeQueueLoadMonitor keeps track of load metrics (such as queue length
 * and total wait time) associated with Container Queues on the Node Manager.
 * It uses this information to periodically sort the Nodes from least to most
 * loaded.
 */
public class NodeQueueLoadMonitor implements ClusterMonitor {

  final static Log LOG = LogFactory.getLog(NodeQueueLoadMonitor.class);

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
        return o1.timestamp < o2.timestamp ? +1 : -1;
      }
      return getMetric(o1) > getMetric(o2) ? +1 : -1;
    }

    public int getMetric(ClusterNode c) {
      return (this == QUEUE_LENGTH) ? c.queueLength : c.queueWaitTime;
    }
  }

  static class ClusterNode {
    int queueLength = 0;
    int queueWaitTime = -1;
    double timestamp;
    final NodeId nodeId;

    public ClusterNode(NodeId nodeId) {
      this.nodeId = nodeId;
      updateTimestamp();
    }

    public ClusterNode setQueueLength(int qLength) {
      this.queueLength = qLength;
      return this;
    }

    public ClusterNode setQueueWaitTime(int wTime) {
      this.queueWaitTime = wTime;
      return this;
    }

    public ClusterNode updateTimestamp() {
      this.timestamp = System.currentTimeMillis();
      return this;
    }
  }

  private final ScheduledExecutorService scheduledExecutor;

  private final List<NodeId> sortedNodes;
  private final Map<NodeId, ClusterNode> clusterNodes =
      new ConcurrentHashMap<>();
  private final LoadComparator comparator;
  private QueueLimitCalculator thresholdCalculator;

  Runnable computeTask = new Runnable() {
    @Override
    public void run() {
      synchronized (sortedNodes) {
        sortedNodes.clear();
        sortedNodes.addAll(sortNodes());
        if (thresholdCalculator != null) {
          thresholdCalculator.update();
        }
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
      LoadComparator comparator) {
    this.sortedNodes = new ArrayList<>();
    this.scheduledExecutor = Executors.newScheduledThreadPool(1);
    this.comparator = comparator;
    this.scheduledExecutor.scheduleAtFixedRate(computeTask,
        nodeComputationInterval, nodeComputationInterval,
        TimeUnit.MILLISECONDS);
  }

  List<NodeId> getSortedNodes() {
    return sortedNodes;
  }

  public QueueLimitCalculator getThresholdCalculator() {
    return thresholdCalculator;
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
  public void addNode(List<NMContainerStatus> containerStatuses, RMNode
      rmNode) {
    LOG.debug("Node added event from: " + rmNode.getNode().getName());
    // Ignoring this currently : at least one NODE_UPDATE heartbeat is
    // required to ensure node eligibility.
  }

  @Override
  public void removeNode(RMNode removedRMNode) {
    LOG.debug("Node delete event for: " + removedRMNode.getNode().getName());
    synchronized (this.clusterNodes) {
      if (this.clusterNodes.containsKey(removedRMNode.getNodeID())) {
        this.clusterNodes.remove(removedRMNode.getNodeID());
        LOG.debug("Delete ClusterNode: " + removedRMNode.getNodeID());
      } else {
        LOG.debug("Node not in list!");
      }
    }
  }

  @Override
  public void updateNode(RMNode rmNode) {
    LOG.debug("Node update event from: " + rmNode.getNodeID());
    QueuedContainersStatus queuedContainersStatus =
        rmNode.getQueuedContainersStatus();
    int estimatedQueueWaitTime =
        queuedContainersStatus.getEstimatedQueueWaitTime();
    int waitQueueLength = queuedContainersStatus.getWaitQueueLength();
    // Add nodes to clusterNodes. If estimatedQueueTime is -1, ignore node
    // UNLESS comparator is based on queue length.
    synchronized (this.clusterNodes) {
      ClusterNode currentNode = this.clusterNodes.get(rmNode.getNodeID());
      if (currentNode == null) {
        if (estimatedQueueWaitTime != -1
            || comparator == LoadComparator.QUEUE_LENGTH) {
          this.clusterNodes.put(rmNode.getNodeID(),
              new ClusterNode(rmNode.getNodeID())
                  .setQueueWaitTime(estimatedQueueWaitTime)
                  .setQueueLength(waitQueueLength));
          LOG.info("Inserting ClusterNode [" + rmNode.getNodeID() + "] " +
              "with queue wait time [" + estimatedQueueWaitTime + "] and " +
              "wait queue length [" + waitQueueLength + "]");
        } else {
          LOG.warn("IGNORING ClusterNode [" + rmNode.getNodeID() + "] " +
              "with queue wait time [" + estimatedQueueWaitTime + "] and " +
              "wait queue length [" + waitQueueLength + "]");
        }
      } else {
        if (estimatedQueueWaitTime != -1
            || comparator == LoadComparator.QUEUE_LENGTH) {
          currentNode
              .setQueueWaitTime(estimatedQueueWaitTime)
              .setQueueLength(waitQueueLength)
              .updateTimestamp();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Updating ClusterNode [" + rmNode.getNodeID() + "] " +
                "with queue wait time [" + estimatedQueueWaitTime + "] and " +
                "wait queue length [" + waitQueueLength + "]");
          }
        } else {
          this.clusterNodes.remove(rmNode.getNodeID());
          LOG.info("Deleting ClusterNode [" + rmNode.getNodeID() + "] " +
              "with queue wait time [" + currentNode.queueWaitTime + "] and " +
              "wait queue length [" + currentNode.queueLength + "]");
        }
      }
    }
  }

  @Override
  public void updateNodeResource(RMNode rmNode, ResourceOption resourceOption) {
    LOG.debug("Node resource update event from: " + rmNode.getNodeID());
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
    synchronized (this.sortedNodes) {
      return ((k < this.sortedNodes.size()) && (k >= 0)) ?
          new ArrayList<>(this.sortedNodes).subList(0, k) :
          new ArrayList<>(this.sortedNodes);
    }
  }

  private List<NodeId> sortNodes() {
    synchronized (this.clusterNodes) {
      ArrayList aList = new ArrayList<>(this.clusterNodes.values());
      List<NodeId> retList = new ArrayList<>();
      Object[] nodes = aList.toArray();
      // Collections.sort would do something similar by calling Arrays.sort
      // internally but would finally iterate through the input list (aList)
      // to reset the value of each element. Since we don't really care about
      // 'aList', we can use the iteration to create the list of nodeIds which
      // is what we ultimately care about.
      Arrays.sort(nodes, (Comparator)comparator);
      for (int j=0; j < nodes.length; j++) {
        retList.add(((ClusterNode)nodes[j]).nodeId);
      }
      return retList;
    }
  }

}
