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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

/**
 * This class extends the application lifecycle management contained with
 * the {@link SchedulerApp} class and adds delay-scheduling information
 * specific to the FairScheduler.
 */
public class FSSchedulerApp extends SchedulerApp {
  private static final Log LOG = LogFactory.getLog(SchedulerApp.class);

  /**
   * Delay scheduling: We often want to prioritize scheduling of node-local
   * containers over rack-local or off-switch containers. To acheive this
   * we first only allow node-local assigments for a given prioirty level,
   * then relax the locality threshold once we've had a long enough period
   * without succesfully scheduling. We measure both the number of "missed"
   * scheduling opportunities since the last container was scheduled
   * at the current allowed level and the time since the last container
   * was scheduled. Currently we use only the former.
   */

  // Current locality threshold
  final Map<Priority, NodeType> allowedLocalityLevel = new HashMap<
      Priority, NodeType>();

  // Time of the last container scheduled at the current allowed level
  Map<Priority, Long> lastScheduledContainer = new HashMap<Priority, Long>();

  public FSSchedulerApp(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext, ApplicationStore store) {
    super(applicationAttemptId, user, queue, activeUsersManager,
        rmContext, store);
  }

  /**
   * Should be called when an application has successfully scheduled a container,
   * or when the scheduling locality threshold is relaxed.
   * Reset various internal counters which affect delay scheduling
   *
   * @param priority The priority of the container scheduled.
   */
  synchronized public void resetSchedulingOpportunities(Priority priority) {
    this.lastScheduledContainer.put(priority, System.currentTimeMillis());
    super.resetSchedulingOpportunities(priority);
  }

  /**
   * Return the level at which we are allowed to schedule containers, given the
   * current size of the cluster and thresholds indicating how many nodes to
   * fail at (as a fraction of cluster size) before relaxing scheduling
   * constraints.
   */
  public synchronized NodeType getAllowedLocalityLevel(Priority priority,
      int numNodes, double nodeLocalityThreshold, double rackLocalityThreshold) {
    // upper limit on threshold
    if (nodeLocalityThreshold > 1.0) { nodeLocalityThreshold = 1.0; }
    if (rackLocalityThreshold > 1.0) { rackLocalityThreshold = 1.0; }

    // If delay scheduling is not being used, can schedule anywhere
    if (nodeLocalityThreshold < 0.0 || rackLocalityThreshold < 0.0) {
      return NodeType.OFF_SWITCH;
    }

    // Default level is NODE_LOCAL
    if (!allowedLocalityLevel.containsKey(priority)) {
      allowedLocalityLevel.put(priority, NodeType.NODE_LOCAL);
      return NodeType.NODE_LOCAL;
    }

    NodeType allowed = allowedLocalityLevel.get(priority);

    // If level is already most liberal, we're done
    if (allowed.equals(NodeType.OFF_SWITCH)) return NodeType.OFF_SWITCH;

    double threshold = allowed.equals(NodeType.NODE_LOCAL) ? nodeLocalityThreshold :
      rackLocalityThreshold;

    // Relax locality constraints once we've surpassed threshold.
    if (this.getSchedulingOpportunities(priority) > (numNodes * threshold)) {
      if (allowed.equals(NodeType.NODE_LOCAL)) {
        allowedLocalityLevel.put(priority, NodeType.RACK_LOCAL);
        this.resetSchedulingOpportunities(priority);
      }
      else if (allowed.equals(NodeType.RACK_LOCAL)) {
        allowedLocalityLevel.put(priority, NodeType.OFF_SWITCH);
        this.resetSchedulingOpportunities(priority);
      }
    }
    return allowedLocalityLevel.get(priority);
  }


  synchronized public RMContainer allocate(NodeType type, SchedulerNode node,
      Priority priority, ResourceRequest request,
      Container container) {
    // Update allowed locality level
    NodeType allowed = this.allowedLocalityLevel.get(priority);
    if (allowed != null) {
      if (allowed.equals(NodeType.OFF_SWITCH) &&
          (type.equals(NodeType.NODE_LOCAL) ||
              type.equals(NodeType.RACK_LOCAL))) {
        this.resetAllowedLocalityLevel(priority, type);
      }
      else if (allowed.equals(NodeType.RACK_LOCAL) &&
          type.equals(NodeType.NODE_LOCAL)) {
        this.resetAllowedLocalityLevel(priority, type);
      }
    }
    return super.allocate(type, node, priority, request, container);
  }

  /**
   * Should be called when the scheduler assigns a container at a higher
   * degree of locality than the current threshold. Reset the allowed locality
   * level to a higher degree of locality.
   */
  public synchronized void resetAllowedLocalityLevel(Priority priority,
      NodeType level) {
    NodeType old = allowedLocalityLevel.get(priority);
    LOG.info("Raising locality level from " + old + " to " + level + " at " +
        " priority " + priority);
    allowedLocalityLevel.put(priority, level);
  }
}
