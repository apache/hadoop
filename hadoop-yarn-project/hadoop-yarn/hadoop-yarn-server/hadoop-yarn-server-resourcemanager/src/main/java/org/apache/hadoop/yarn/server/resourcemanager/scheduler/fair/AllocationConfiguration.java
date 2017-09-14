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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

public class AllocationConfiguration extends ReservationSchedulerConfiguration {
  private static final AccessControlList EVERYBODY_ACL = new AccessControlList("*");
  private static final AccessControlList NOBODY_ACL = new AccessControlList(" ");
  // Minimum resource allocation for each queue
  private final Map<String, Resource> minQueueResources;
  // Maximum amount of resources per queue
  @VisibleForTesting
  final Map<String, ConfigurableResource> maxQueueResources;
  // Maximum amount of resources for each queue's ad hoc children
  private final Map<String, ConfigurableResource> maxChildQueueResources;
  // Sharing weights for each queue
  private final Map<String, Float> queueWeights;
  
  // Max concurrent running applications for each queue and for each user; in addition,
  // for users that have no max specified, we use the userMaxJobsDefault.
  @VisibleForTesting
  final Map<String, Integer> queueMaxApps;
  @VisibleForTesting
  final Map<String, Integer> userMaxApps;
  private final int userMaxAppsDefault;
  private final int queueMaxAppsDefault;
  private final ConfigurableResource queueMaxResourcesDefault;

  // Maximum resource share for each leaf queue that can be used to run AMs
  final Map<String, Float> queueMaxAMShares;
  private final float queueMaxAMShareDefault;

  // ACL's for each queue. Only specifies non-default ACL's from configuration.
  private final Map<String, Map<AccessType, AccessControlList>> queueAcls;

  // Reservation ACL's for each queue. Only specifies non-default ACL's from
  // configuration.
  private final Map<String, Map<ReservationACL, AccessControlList>> resAcls;

  // Min share preemption timeout for each queue in seconds. If a job in the queue
  // waits this long without receiving its guaranteed share, it is allowed to
  // preempt other jobs' tasks.
  private final Map<String, Long> minSharePreemptionTimeouts;

  // Fair share preemption timeout for each queue in seconds. If a job in the
  // queue waits this long without receiving its fair share threshold, it is
  // allowed to preempt other jobs' tasks.
  private final Map<String, Long> fairSharePreemptionTimeouts;

  // The fair share preemption threshold for each queue. If a queue waits
  // fairSharePreemptionTimeout without receiving
  // fairshare * fairSharePreemptionThreshold resources, it is allowed to
  // preempt other queues' tasks.
  private final Map<String, Float> fairSharePreemptionThresholds;

  private final Set<String> reservableQueues;

  private final Map<String, SchedulingPolicy> schedulingPolicies;
  
  private final SchedulingPolicy defaultSchedulingPolicy;
  
  // Policy for mapping apps to queues
  @VisibleForTesting
  QueuePlacementPolicy placementPolicy;
  
  //Configured queues in the alloc xml
  @VisibleForTesting
  Map<FSQueueType, Set<String>> configuredQueues;

  // Reservation system configuration
  private ReservationQueueConfiguration globalReservationQueueConfig;

  private final Set<String> nonPreemptableQueues;

  public AllocationConfiguration(Map<String, Resource> minQueueResources,
      Map<String, ConfigurableResource> maxQueueResources,
      Map<String, ConfigurableResource> maxChildQueueResources,
      Map<String, Integer> queueMaxApps, Map<String, Integer> userMaxApps,
      Map<String, Float> queueWeights,
      Map<String, Float> queueMaxAMShares, int userMaxAppsDefault,
      int queueMaxAppsDefault, ConfigurableResource queueMaxResourcesDefault,
      float queueMaxAMShareDefault,
      Map<String, SchedulingPolicy> schedulingPolicies,
      SchedulingPolicy defaultSchedulingPolicy,
      Map<String, Long> minSharePreemptionTimeouts,
      Map<String, Long> fairSharePreemptionTimeouts,
      Map<String, Float> fairSharePreemptionThresholds,
      Map<String, Map<AccessType, AccessControlList>> queueAcls,
      Map<String, Map<ReservationACL, AccessControlList>> resAcls,
      QueuePlacementPolicy placementPolicy,
      Map<FSQueueType, Set<String>> configuredQueues,
      ReservationQueueConfiguration globalReservationQueueConfig,
      Set<String> reservableQueues,
      Set<String> nonPreemptableQueues) {
    this.minQueueResources = minQueueResources;
    this.maxQueueResources = maxQueueResources;
    this.maxChildQueueResources = maxChildQueueResources;
    this.queueMaxApps = queueMaxApps;
    this.userMaxApps = userMaxApps;
    this.queueMaxAMShares = queueMaxAMShares;
    this.queueWeights = queueWeights;
    this.userMaxAppsDefault = userMaxAppsDefault;
    this.queueMaxResourcesDefault = queueMaxResourcesDefault;
    this.queueMaxAppsDefault = queueMaxAppsDefault;
    this.queueMaxAMShareDefault = queueMaxAMShareDefault;
    this.defaultSchedulingPolicy = defaultSchedulingPolicy;
    this.schedulingPolicies = schedulingPolicies;
    this.minSharePreemptionTimeouts = minSharePreemptionTimeouts;
    this.fairSharePreemptionTimeouts = fairSharePreemptionTimeouts;
    this.fairSharePreemptionThresholds = fairSharePreemptionThresholds;
    this.queueAcls = queueAcls;
    this.resAcls = resAcls;
    this.reservableQueues = reservableQueues;
    this.globalReservationQueueConfig = globalReservationQueueConfig;
    this.placementPolicy = placementPolicy;
    this.configuredQueues = configuredQueues;
    this.nonPreemptableQueues = nonPreemptableQueues;
  }
  
  public AllocationConfiguration(Configuration conf) {
    minQueueResources = new HashMap<>();
    maxChildQueueResources = new HashMap<>();
    maxQueueResources = new HashMap<>();
    queueWeights = new HashMap<>();
    queueMaxApps = new HashMap<>();
    userMaxApps = new HashMap<>();
    queueMaxAMShares = new HashMap<>();
    userMaxAppsDefault = Integer.MAX_VALUE;
    queueMaxAppsDefault = Integer.MAX_VALUE;
    queueMaxResourcesDefault = new ConfigurableResource(Resources.unbounded());
    queueMaxAMShareDefault = 0.5f;
    queueAcls = new HashMap<>();
    resAcls = new HashMap<>();
    minSharePreemptionTimeouts = new HashMap<>();
    fairSharePreemptionTimeouts = new HashMap<>();
    fairSharePreemptionThresholds = new HashMap<>();
    schedulingPolicies = new HashMap<>();
    defaultSchedulingPolicy = SchedulingPolicy.DEFAULT_POLICY;
    reservableQueues = new HashSet<>();
    configuredQueues = new HashMap<>();
    for (FSQueueType queueType : FSQueueType.values()) {
      configuredQueues.put(queueType, new HashSet<>());
    }
    placementPolicy =
        QueuePlacementPolicy.fromConfiguration(conf, configuredQueues);
    nonPreemptableQueues = new HashSet<>();
  }
  
  /**
   * Get the ACLs associated with this queue. If a given ACL is not explicitly
   * configured, include the default value for that ACL.  The default for the
   * root queue is everybody ("*") and the default for all other queues is
   * nobody ("")
   */
  public AccessControlList getQueueAcl(String queue, QueueACL operation) {
    Map<AccessType, AccessControlList> acls = this.queueAcls.get(queue);
    if (acls != null) {
      AccessControlList operationAcl =
          acls.get(SchedulerUtils.toAccessType(operation));
      if (operationAcl != null) {
        return operationAcl;
      }
    }
    return (queue.equals("root")) ? EVERYBODY_ACL : NOBODY_ACL;
  }

  /**
   * Get the map of ACLs of all queues.
   * @return the map of ACLs of all queues
   */
  public Map<String, Map<AccessType, AccessControlList>> getQueueAcls() {
    return Collections.unmodifiableMap(this.queueAcls);
  }

  @Override
  /**
   * Get the map of reservation ACLs to {@link AccessControlList} for the
   * specified queue.
   */
  public Map<ReservationACL, AccessControlList> getReservationAcls(String
        queue) {
    return this.resAcls.get(queue);
  }

  /**
   * Get a queue's min share preemption timeout configured in the allocation
   * file, in milliseconds. Return -1 if not set.
   */
  public long getMinSharePreemptionTimeout(String queueName) {
    Long minSharePreemptionTimeout = minSharePreemptionTimeouts.get(queueName);
    return (minSharePreemptionTimeout == null) ? -1 : minSharePreemptionTimeout;
  }

  /**
   * Get a queue's fair share preemption timeout configured in the allocation
   * file, in milliseconds. Return -1 if not set.
   */
  public long getFairSharePreemptionTimeout(String queueName) {
    Long fairSharePreemptionTimeout = fairSharePreemptionTimeouts.get(queueName);
    return (fairSharePreemptionTimeout == null) ?
        -1 : fairSharePreemptionTimeout;
  }

  /**
   * Get a queue's fair share preemption threshold in the allocation file.
   * Return -1f if not set.
   */
  public float getFairSharePreemptionThreshold(String queueName) {
    Float fairSharePreemptionThreshold =
        fairSharePreemptionThresholds.get(queueName);
    return (fairSharePreemptionThreshold == null) ?
        -1f : fairSharePreemptionThreshold;
  }

  public boolean isPreemptable(String queueName) {
    return !nonPreemptableQueues.contains(queueName);
  }

  private float getQueueWeight(String queue) {
    Float weight = queueWeights.get(queue);
    return (weight == null) ? 1.0f : weight;
  }

  public int getUserMaxApps(String user) {
    Integer maxApps = userMaxApps.get(user);
    return (maxApps == null) ? userMaxAppsDefault : maxApps;
  }

  @VisibleForTesting
  int getQueueMaxApps(String queue) {
    Integer maxApps = queueMaxApps.get(queue);
    return (maxApps == null) ? queueMaxAppsDefault : maxApps;
  }

  @VisibleForTesting
  float getQueueMaxAMShare(String queue) {
    Float maxAMShare = queueMaxAMShares.get(queue);
    return (maxAMShare == null) ? queueMaxAMShareDefault : maxAMShare;
  }

  /**
   * Get the minimum resource allocation for the given queue.
   *
   * @param queue the target queue's name
   * @return the min allocation on this queue or {@link Resources#none}
   * if not set
   */
  @VisibleForTesting
  Resource getMinResources(String queue) {
    Resource minQueueResource = minQueueResources.get(queue);
    return (minQueueResource == null) ? Resources.none() : minQueueResource;
  }

  /**
   * Get the maximum resource allocation for the given queue. If the max in not
   * set, return the default max.
   *
   * @param queue the target queue's name
   * @return the max allocation on this queue
   */
  @VisibleForTesting
  ConfigurableResource getMaxResources(String queue) {
    ConfigurableResource maxQueueResource = maxQueueResources.get(queue);
    if (maxQueueResource == null) {
      maxQueueResource = queueMaxResourcesDefault;
    }
    return maxQueueResource;
  }
  
  /**
   * Get the maximum resource allocation for children of the given queue.
   *
   * @param queue the target queue's name
   * @return the max allocation on this queue or null if not set
   */
  @VisibleForTesting
  ConfigurableResource getMaxChildResources(String queue) {
    return maxChildQueueResources.get(queue);
  }

  @VisibleForTesting
  SchedulingPolicy getSchedulingPolicy(String queueName) {
    SchedulingPolicy policy = schedulingPolicies.get(queueName);
    return (policy == null) ? defaultSchedulingPolicy : policy;
  }
  
  public SchedulingPolicy getDefaultSchedulingPolicy() {
    return defaultSchedulingPolicy;
  }
  
  public Map<FSQueueType, Set<String>> getConfiguredQueues() {
    return configuredQueues;
  }
  
  public QueuePlacementPolicy getPlacementPolicy() {
    return placementPolicy;
  }

  @Override
  public boolean isReservable(String queue) {
    return reservableQueues.contains(queue);
  }

  @Override
  public long getReservationWindow(String queue) {
    return globalReservationQueueConfig.getReservationWindowMsec();
  }

  @Override
  public float getAverageCapacity(String queue) {
    return globalReservationQueueConfig.getAvgOverTimeMultiplier() * 100;
  }

  @Override
  public float getInstantaneousMaxCapacity(String queue) {
    return globalReservationQueueConfig.getMaxOverTimeMultiplier() * 100;
  }

  @Override
  public String getReservationAdmissionPolicy(String queue) {
    return globalReservationQueueConfig.getReservationAdmissionPolicy();
  }

  @Override
  public String getReservationAgent(String queue) {
    return globalReservationQueueConfig.getReservationAgent();
  }

  @Override
  public boolean getShowReservationAsQueues(String queue) {
    return globalReservationQueueConfig.shouldShowReservationAsQueues();
  }

  @Override
  public String getReplanner(String queue) {
    return globalReservationQueueConfig.getPlanner();
  }

  @Override
  public boolean getMoveOnExpiry(String queue) {
    return globalReservationQueueConfig.shouldMoveOnExpiry();
  }

  @Override
  public long getEnforcementWindow(String queue) {
    return globalReservationQueueConfig.getEnforcementWindowMsec();
  }

  @VisibleForTesting
  public void setReservationWindow(long window) {
    globalReservationQueueConfig.setReservationWindow(window);
  }

  @VisibleForTesting
  public void setAverageCapacity(int avgCapacity) {
    globalReservationQueueConfig.setAverageCapacity(avgCapacity);
  }

  /**
   * Initialize a {@link FSQueue} with queue-specific properties and its
   * metrics.
   * @param queue the FSQueue needed to be initialized
   */
  public void initFSQueue(FSQueue queue){
    // Set queue-specific properties.
    String name = queue.getName();
    queue.setWeights(getQueueWeight(name));
    queue.setMinShare(getMinResources(name));
    queue.setMaxShare(getMaxResources(name));
    queue.setMaxRunningApps(getQueueMaxApps(name));
    queue.setMaxAMShare(getQueueMaxAMShare(name));
    queue.setMaxChildQueueResource(getMaxChildResources(name));

    // Set queue metrics.
    queue.getMetrics().setMinShare(queue.getMinShare());
    queue.getMetrics().setMaxShare(queue.getMaxShare());
    queue.getMetrics().setMaxApps(queue.getMaxRunningApps());
    queue.getMetrics().setSchedulingPolicy(getSchedulingPolicy(name).getName());
  }
}
