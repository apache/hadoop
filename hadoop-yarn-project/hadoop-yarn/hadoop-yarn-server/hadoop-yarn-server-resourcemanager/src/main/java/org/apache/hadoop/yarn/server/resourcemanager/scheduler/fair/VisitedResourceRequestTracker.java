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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ClusterNodeTracker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Applications place {@link ResourceRequest}s at multiple levels. This is a
 * helper class that allows tracking if a {@link ResourceRequest} has been
 * visited at a different locality level.
 *
 * This is implemented for {@link FSAppAttempt#getStarvedResourceRequests()}.
 * The implementation is not thread-safe.
 */
class VisitedResourceRequestTracker {
  private final Map<Priority, Map<Resource, TrackerPerPriorityResource>> map =
      new HashMap<>();
  private final ClusterNodeTracker<FSSchedulerNode> nodeTracker;

  VisitedResourceRequestTracker(
      ClusterNodeTracker<FSSchedulerNode> nodeTracker) {
    this.nodeTracker = nodeTracker;
  }

  /**
   * Check if the {@link ResourceRequest} is visited before, and track it.
   * @param rr {@link ResourceRequest} to visit
   * @return true if <code>rr</code> this is the first visit across all
   * locality levels, false otherwise
   */
  boolean visit(ResourceRequest rr) {
    Priority priority = rr.getPriority();
    Resource capability = rr.getCapability();

    Map<Resource, TrackerPerPriorityResource> subMap = map.get(priority);
    if (subMap == null) {
      subMap = new HashMap<>();
      map.put(priority, subMap);
    }

    TrackerPerPriorityResource tracker = subMap.get(capability);
    if (tracker == null) {
      tracker = new TrackerPerPriorityResource();
      subMap.put(capability, tracker);
    }

    return tracker.visit(rr.getResourceName());
  }

  private class TrackerPerPriorityResource {
    private Set<String> racksWithNodesVisited = new HashSet<>();
    private Set<String> racksVisted = new HashSet<>();
    private boolean anyVisited;

    private boolean visitAny() {
      if (racksVisted.isEmpty() && racksWithNodesVisited.isEmpty()) {
        anyVisited = true;
      }
      return anyVisited;
    }

    private boolean visitRack(String rackName) {
      if (anyVisited || racksWithNodesVisited.contains(rackName)) {
        return false;
      } else {
        racksVisted.add(rackName);
        return true;
      }
    }

    private boolean visitNode(String rackName) {
      if (anyVisited || racksVisted.contains(rackName)) {
        return false;
      } else {
        racksWithNodesVisited.add(rackName);
        return true;
      }
    }

    private boolean visit(String resourceName) {
      if (resourceName.equals(ResourceRequest.ANY)) {
        return visitAny();
      }

      List<FSSchedulerNode> nodes =
          nodeTracker.getNodesByResourceName(resourceName);
      switch (nodes.size()) {
        case 0:
          // Log error
          return false;
        case 1:
          // Node
          return visitNode(nodes.remove(0).getRackName());
        default:
          // Rack
          return visitRack(resourceName);
      }
    }
  }
}
