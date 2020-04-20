/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NodeInstanceTypeBinPackingPolicy does sorting based on Node Utilization
 * and Node Instance Type. ApplicationMaster Schedule will get Node Iterator
 * with Worker Nodes followed by Compute Nodes with internally sorted
 * based on Node Utilization by LayeredNodeUsageBinPackingPolicy.
 * Task Containers will get Node Iterator with Compute Nodes followed
 * by Worker Nodes.
 * */
public class NodeInstanceTypeBinPackingPolicy<N extends SchedulerNode>
    extends LayeredNodeUsageBinPackingPolicy<N> {

    private static final Logger LOG =
        LoggerFactory.getLogger(NodeInstanceTypeBinPackingPolicy.class);
    private static final String WORKER = "worker";

    protected Map<String, Set<N>> amNodesPerPartition = new ConcurrentHashMap<>();
    protected Map<String, Set<N>> taskNodesPerPartition = new ConcurrentHashMap<>();

    public NodeInstanceTypeBinPackingPolicy() {
      super();
    }

    @Override
    public Iterator<N> getPreferredNodeIterator(Collection<N> nodes,
      String partition, SchedulerApplicationAttempt appAttempt) {
      LOG.debug("Node Iterator for "
          + (appAttempt.isWaitingForAMContainer() ? "AM" : "Task")
          +  " of " + appAttempt.getId());
      if (appAttempt.isWaitingForAMContainer()) {
        return amNodesPerPartition.getOrDefault(partition,
            Collections.emptySet()).iterator();
      }
      return getNodesPerPartition(partition).iterator();
    }

    @Override
    public void addAndRefreshNodesSet(Collection<N> nodes,
        String partition) {
      List<N> computeNodes = new ArrayList<>();
      List<N> workerNodes = new ArrayList<>();

      for (N node : nodes) {
        nodeToScore.put(node.getNodeID().toString(), calculateScore(node));
        if (node.getNodeName().contains(WORKER)) {
          workerNodes.add(node);
        } else {
          computeNodes.add(node);
        }
      }

      Collections.sort(computeNodes, comparator);
      Collections.sort(workerNodes, comparator);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Worker Nodes after sorting: " + workerNodes);
        LOG.debug("Compute Nodes after sorting: " + computeNodes);
      }

      Set<N> nodeListForTask = new LinkedHashSet<>();
      nodeListForTask.addAll(computeNodes);
      nodeListForTask.addAll(workerNodes);
      taskNodesPerPartition.put(partition,
          Collections.unmodifiableSet(nodeListForTask));

      Set<N> nodeListForAM = new LinkedHashSet<>();
      nodeListForAM.addAll(workerNodes);
      nodeListForAM.addAll(computeNodes);
      amNodesPerPartition.put(partition,
          Collections.unmodifiableSet(nodeListForAM));
    }

    @Override
    public Set<N> getNodesPerPartition(String partition) {
      return taskNodesPerPartition.getOrDefault(partition,
          Collections.emptySet());
    }
}
