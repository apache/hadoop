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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;


/**
 * Maintains a list of nodes based on node resource usage for bin-packing policy
 * without too much hot spots.
 *
 * The node list can be viewed in three parts. The normal, busy and low.
 * Each parts consists nodes with same usage range. For instance,
 * the normal usage nodes are from 0 to 0.6 (doesn't contains 0) and sorted in
 * descending order. The busy usage nodes are from 0.6 to 1 and sorted in
 * ascending order to avoid hot spot in this layer. The free usage nodes are not
 * running any containers and be safe to decommission.
 *
 * Layer      Normal              High           Free
 * Usage      (0%-60%)           >60%             0%
 * Order     Descending        Ascending      random
 *
 * When global scheduler trying to schedule container to node,
 * this list will be iterated. The first priority is normal layer, then busy
 * and free layers.
 * */
public class LayeredNodeUsageBinPackingPolicy<N extends SchedulerNode>
    implements MultiNodeLookupPolicy<N> {

  private static final Logger LOG =
      LoggerFactory.getLogger(LayeredNodeUsageBinPackingPolicy.class);


  protected Map<String, Set<N>> nodesPerPartition = new ConcurrentHashMap<>();

  protected Map<String, Float> nodeToScore = new HashMap<>();

  // scorer related parameters
  private final float normalLayerMinimumScore = 2000;
  private final float normalLayerSlope = 3;
  private final float busyLayerMaxScore = 1000;
  private final float busyLayerSlope = -3;
  private final float busyLayerThreshold = (float)0.6;

  protected Comparator<N> comparator;

  public LayeredNodeUsageBinPackingPolicy(boolean debug) {}

  public LayeredNodeUsageBinPackingPolicy() {
    this.comparator = new Comparator<N>() {
      @Override
      public int compare(N o1, N o2) {
        float score1 = nodeToScore.get(o1.getNodeID().toString());
        float score2 = nodeToScore.get(o2.getNodeID().toString());
        float allocatedDiff = score2 - score1;
        if (allocatedDiff == 0) {
          return o1.getNodeID().compareTo(o2.getNodeID());
        }
        return allocatedDiff > 0 ? 1 : -1;
      }
    };
  }

  private float calculateScore(N node) {
    LOG.debug("{}'s used resource is {}", node.getNodeID().toString(), node.getAllocatedResource());
    float nodeResourceUsage = getNodeResourceUsage(node.getAllocatedResource(),
        node.getTotalResource());
    LOG.debug("{}'s resource usage is {}", node.getNodeID().toString(), nodeResourceUsage);
    float score = scorer(nodeResourceUsage, busyLayerThreshold);
    LOG.debug("{}'s final score is {}", node.getNodeID().toString(), score);
    return score;
  }

  /**
   * Essentially, the order of normal, busy and free usage nodes can be unified
   * through a Piecewise function.
   *
   * score
   *   |
   *   |      /
   *   |     /
   *   |    /
   *   |   /(normal layer)
   *   |  /
   *   | /
   *   |/
   *   |
   *   |
   *   |        \
   *   |         \ (busy)
   *   |          \
   *   |
   *   |
   *   |__(free)
   *   ------------------ usage (%)
   *   0     60   100
   * The function accept a node usage, busy threshold and output a score.
   * */
  public float scorer(float resourceUsage, float busyLayerThreshold) {
    // fall in normal layer, the higher resource usage, the higher score
    if (resourceUsage < busyLayerThreshold && resourceUsage > 0) {
      return normalLayerMinimumScore + normalLayerSlope * resourceUsage * 100;
    } else if (resourceUsage >= busyLayerThreshold) {
      // fall in busy layer. The higher resource usage, the lower score
      return busyLayerMaxScore + busyLayerSlope * resourceUsage * 100;
    } else {
      // fall in free layer. Get a random score. Max 100
      return new Random().nextFloat() * 100;
    }
  }

  /**
   * Pick the highest usage based on memory and cpu.
   * */
  private float getNodeResourceUsage(Resource r, Resource base) {
    float memUsage = (float)r.getMemorySize()/(float)base.getMemorySize();
    float cpuUsage = (float)r.getVirtualCores()/(float)base.getVirtualCores();
    return memUsage > cpuUsage ? memUsage : cpuUsage;
  }

  @Override
  public Iterator<N> getPreferredNodeIterator(Collection<N> nodes,
      String partition) {
    return getNodesPerPartition(partition).iterator();
  }

  @Override
  public void addAndRefreshNodesSet(Collection<N> nodes,
      String partition) {
    // Compute node's score
    for (N node : nodes) {
      nodeToScore.put(node.getNodeID().toString(), calculateScore(node));
    }

    Set<N> nodeList = new ConcurrentSkipListSet<N>(comparator);
    nodeList.addAll(nodes);
    nodesPerPartition.put(partition, Collections.unmodifiableSet(nodeList));
  }

  @Override
  public Set<N> getNodesPerPartition(String partition) {
    return nodesPerPartition.getOrDefault(partition, Collections.emptySet());
  }
}
