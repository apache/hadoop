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
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TopKNodeSelector implements ClusterMonitor {

  final static Log LOG = LogFactory.getLog(TopKNodeSelector.class);

  public enum TopKComparator implements Comparator<ClusterNode> {
    WAIT_TIME,
    QUEUE_LENGTH;

    @Override
    public int compare(ClusterNode o1, ClusterNode o2) {
      if (getQuant(o1) == getQuant(o2)) {
        return o1.timestamp < o2.timestamp ? +1 : -1;
      }
      return getQuant(o1) > getQuant(o2) ? +1 : -1;
    }

    private int getQuant(ClusterNode c) {
      return (this == WAIT_TIME) ? c.queueTime : c.waitQueueLength;
    }
  }

  static class ClusterNode {
    int queueTime = -1;
    int waitQueueLength = 0;
    double timestamp;
    final NodeId nodeId;

    public ClusterNode(NodeId nodeId) {
      this.nodeId = nodeId;
      updateTimestamp();
    }

    public ClusterNode setQueueTime(int queueTime) {
      this.queueTime = queueTime;
      return this;
    }

    public ClusterNode setWaitQueueLength(int queueLength) {
      this.waitQueueLength = queueLength;
      return this;
    }

    public ClusterNode updateTimestamp() {
      this.timestamp = System.currentTimeMillis();
      return this;
    }
  }

  private final int k;
  private final List<NodeId> topKNodes;
  private final ScheduledExecutorService scheduledExecutor;
  private final HashMap<NodeId, ClusterNode> clusterNodes = new HashMap<>();
  private final Comparator<ClusterNode> comparator;

  Runnable computeTask = new Runnable() {
    @Override
    public void run() {
      synchronized (topKNodes) {
        topKNodes.clear();
        topKNodes.addAll(computeTopKNodes());
      }
    }
  };

  @VisibleForTesting
  TopKNodeSelector(int k, TopKComparator comparator) {
    this.k = k;
    this.topKNodes = new ArrayList<>();
    this.comparator = comparator;
    this.scheduledExecutor = null;
  }

  public TopKNodeSelector(int k, long nodeComputationInterval,
      TopKComparator comparator) {
    this.k = k;
    this.topKNodes = new ArrayList<>();
    this.scheduledExecutor = Executors.newScheduledThreadPool(1);
    this.comparator = comparator;
    this.scheduledExecutor.scheduleAtFixedRate(computeTask,
        nodeComputationInterval, nodeComputationInterval,
        TimeUnit.MILLISECONDS);
  }


  @Override
  public void addNode(List<NMContainerStatus> containerStatuses, RMNode
      rmNode) {
    LOG.debug("Node added event from: " + rmNode.getNode().getName());
    // Ignoring this currently : atleast one NODE_UPDATE heartbeat is
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
  public void nodeUpdate(RMNode rmNode) {
    LOG.debug("Node update event from: " + rmNode.getNodeID());
    QueuedContainersStatus queuedContainersStatus =
        rmNode.getQueuedContainersStatus();
    int estimatedQueueWaitTime =
        queuedContainersStatus.getEstimatedQueueWaitTime();
    int waitQueueLength = queuedContainersStatus.getWaitQueueLength();
    // Add nodes to clusterNodes.. if estimatedQueueTime is -1, Ignore node
    // UNLESS comparator is based on queue length, in which case, we should add
    synchronized (this.clusterNodes) {
      ClusterNode currentNode = this.clusterNodes.get(rmNode.getNodeID());
      if (currentNode == null) {
        if (estimatedQueueWaitTime != -1
            || comparator == TopKComparator.QUEUE_LENGTH) {
          this.clusterNodes.put(rmNode.getNodeID(),
              new ClusterNode(rmNode.getNodeID())
                  .setQueueTime(estimatedQueueWaitTime)
                  .setWaitQueueLength(waitQueueLength));
          LOG.info("Inserting ClusterNode [" + rmNode.getNodeID() + "]" +
              "with queue wait time [" + estimatedQueueWaitTime + "] and " +
              "wait queue length [" + waitQueueLength + "]");
        } else {
          LOG.warn("IGNORING ClusterNode [" + rmNode.getNodeID() + "]" +
              "with queue wait time [" + estimatedQueueWaitTime + "] and " +
              "wait queue length [" + waitQueueLength + "]");
        }
      } else {
        if (estimatedQueueWaitTime != -1
            || comparator == TopKComparator.QUEUE_LENGTH) {
          currentNode
              .setQueueTime(estimatedQueueWaitTime)
              .setWaitQueueLength(waitQueueLength)
              .updateTimestamp();
          LOG.info("Updating ClusterNode [" + rmNode.getNodeID() + "]" +
              "with queue wait time [" + estimatedQueueWaitTime + "] and " +
              "wait queue length [" + waitQueueLength + "]");
        } else {
          this.clusterNodes.remove(rmNode.getNodeID());
          LOG.info("Deleting ClusterNode [" + rmNode.getNodeID() + "]" +
              "with queue wait time [" + currentNode.queueTime + "] and " +
              "wait queue length [" + currentNode.waitQueueLength + "]");
        }
      }
    }
  }

  @Override
  public void updateNodeResource(RMNode rmNode, ResourceOption resourceOption) {
    LOG.debug("Node resource update event from: " + rmNode.getNodeID());
    // Ignoring this currently...
  }

  public List<NodeId> selectNodes() {
    synchronized (this.topKNodes) {
      return this.k < this.topKNodes.size() ?
          new ArrayList<>(this.topKNodes).subList(0, this.k) :
          new ArrayList<>(this.topKNodes);
    }
  }

  private List<NodeId> computeTopKNodes() {
    synchronized (this.clusterNodes) {
      ArrayList aList = new ArrayList<>(this.clusterNodes.values());
      List<NodeId> retList = new ArrayList<>();
      Object[] nodes = aList.toArray();
      // Collections.sort would do something similar by calling Arrays.sort
      // internally but would finally iterate through the input list (aList)
      // to reset the value of each element.. Since we don't really care about
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
