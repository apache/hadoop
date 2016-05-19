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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.NodeQueueLoadMonitor.ClusterNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.NodeQueueLoadMonitor.LoadComparator;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class interacts with the NodeQueueLoadMonitor to keep track of the
 * mean and standard deviation of the configured metrics (queue length or queue
 * wait time) used to characterize the queue load of a specific node.
 * The NodeQueueLoadMonitor triggers an update (by calling the
 * <code>update()</code> method) every time it performs a re-ordering of
 * all nodes.
 */
public class QueueLimitCalculator {

  class Stats {
    private final AtomicInteger mean = new AtomicInteger(0);
    private final AtomicInteger stdev = new AtomicInteger(0);

    /**
     * Not thread safe. Caller should synchronize on sorted nodes list.
     */
    void update() {
      List<NodeId> sortedNodes = nodeSelector.getSortedNodes();
      if (sortedNodes.size() > 0) {
        // Calculate mean
        int sum = 0;
        for (NodeId n : sortedNodes) {
          sum += getMetric(getNode(n));
        }
        mean.set(sum / sortedNodes.size());

        // Calculate stdev
        int sqrSumMean = 0;
        for (NodeId n : sortedNodes) {
          int val = getMetric(getNode(n));
          sqrSumMean += Math.pow(val - mean.get(), 2);
        }
        stdev.set(
            (int) Math.round(Math.sqrt(
                sqrSumMean / (float) sortedNodes.size())));
      }
    }

    private ClusterNode getNode(NodeId nId) {
      return nodeSelector.getClusterNodes().get(nId);
    }

    private int getMetric(ClusterNode cn) {
      return (cn != null) ? ((LoadComparator)nodeSelector.getComparator())
              .getMetric(cn) : 0;
    }

    public int getMean() {
      return mean.get();
    }

    public int getStdev() {
      return stdev.get();
    }
  }

  private final NodeQueueLoadMonitor nodeSelector;
  private final float sigma;
  private final int rangeMin;
  private final int rangeMax;
  private final Stats stats = new Stats();

  QueueLimitCalculator(NodeQueueLoadMonitor selector, float sigma,
      int rangeMin, int rangeMax) {
    this.nodeSelector = selector;
    this.sigma = sigma;
    this.rangeMax = rangeMax;
    this.rangeMin = rangeMin;
  }

  private int determineThreshold() {
    return (int) (stats.getMean() + sigma * stats.getStdev());
  }

  void update() {
    this.stats.update();
  }

  private int getThreshold() {
    int thres = determineThreshold();
    return Math.min(rangeMax, Math.max(rangeMin, thres));
  }

  public ContainerQueuingLimit createContainerQueuingLimit() {
    ContainerQueuingLimit containerQueuingLimit =
        ContainerQueuingLimit.newInstance();
    if (nodeSelector.getComparator() == LoadComparator.QUEUE_WAIT_TIME) {
      containerQueuingLimit.setMaxQueueWaitTimeInMs(getThreshold());
      containerQueuingLimit.setMaxQueueLength(-1);
    } else {
      containerQueuingLimit.setMaxQueueWaitTimeInMs(-1);
      containerQueuingLimit.setMaxQueueLength(getThreshold());
    }
    return containerQueuingLimit;
  }
}
