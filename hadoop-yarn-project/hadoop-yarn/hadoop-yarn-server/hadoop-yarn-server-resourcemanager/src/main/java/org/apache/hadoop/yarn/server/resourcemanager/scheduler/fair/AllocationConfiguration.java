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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

public class AllocationConfiguration {
  private static final AccessControlList EVERYBODY_ACL = new AccessControlList("*");
  private static final AccessControlList NOBODY_ACL = new AccessControlList(" ");
  
  // Minimum resource allocation for each queue
  private final Map<String, Resource> minQueueResources;
  // Maximum amount of resources per queue
  @VisibleForTesting
  final Map<String, Resource> maxQueueResources;
  // Sharing weights for each queue
  private final Map<String, ResourceWeights> queueWeights;
  
  // Max concurrent running applications for each queue and for each user; in addition,
  // for users that have no max specified, we use the userMaxJobsDefault.
  @VisibleForTesting
  final Map<String, Integer> queueMaxApps;
  @VisibleForTesting
  final Map<String, Integer> userMaxApps;
  private final int userMaxAppsDefault;
  private final int queueMaxAppsDefault;

  // ACL's for each queue. Only specifies non-default ACL's from configuration.
  private final Map<String, Map<QueueACL, AccessControlList>> queueAcls;

  // Min share preemption timeout for each queue in seconds. If a job in the queue
  // waits this long without receiving its guaranteed share, it is allowed to
  // preempt other jobs' tasks.
  private final Map<String, Long> minSharePreemptionTimeouts;

  // Default min share preemption timeout for queues where it is not set
  // explicitly.
  private final long defaultMinSharePreemptionTimeout;

  // Preemption timeout for jobs below fair share in seconds. If a job remains
  // below half its fair share for this long, it is allowed to preempt tasks.
  private final long fairSharePreemptionTimeout;

  private final Map<String, SchedulingPolicy> schedulingPolicies;
  
  private final SchedulingPolicy defaultSchedulingPolicy;
  
  // Policy for mapping apps to queues
  @VisibleForTesting
  QueuePlacementPolicy placementPolicy;
  
  @VisibleForTesting
  Set<String> queueNames;
  
  public AllocationConfiguration(Map<String, Resource> minQueueResources, 
      Map<String, Resource> maxQueueResources, 
      Map<String, Integer> queueMaxApps, Map<String, Integer> userMaxApps,
      Map<String, ResourceWeights> queueWeights, int userMaxAppsDefault,
      int queueMaxAppsDefault, Map<String, SchedulingPolicy> schedulingPolicies,
      SchedulingPolicy defaultSchedulingPolicy,
      Map<String, Long> minSharePreemptionTimeouts, 
      Map<String, Map<QueueACL, AccessControlList>> queueAcls,
      long fairSharePreemptionTimeout, long defaultMinSharePreemptionTimeout,
      QueuePlacementPolicy placementPolicy, Set<String> queueNames) {
    this.minQueueResources = minQueueResources;
    this.maxQueueResources = maxQueueResources;
    this.queueMaxApps = queueMaxApps;
    this.userMaxApps = userMaxApps;
    this.queueWeights = queueWeights;
    this.userMaxAppsDefault = userMaxAppsDefault;
    this.queueMaxAppsDefault = queueMaxAppsDefault;
    this.defaultSchedulingPolicy = defaultSchedulingPolicy;
    this.schedulingPolicies = schedulingPolicies;
    this.minSharePreemptionTimeouts = minSharePreemptionTimeouts;
    this.queueAcls = queueAcls;
    this.fairSharePreemptionTimeout = fairSharePreemptionTimeout;
    this.defaultMinSharePreemptionTimeout = defaultMinSharePreemptionTimeout;
    this.placementPolicy = placementPolicy;
    this.queueNames = queueNames;
  }
  
  public AllocationConfiguration(Configuration conf) {
    minQueueResources = new HashMap<String, Resource>();
    maxQueueResources = new HashMap<String, Resource>();
    queueWeights = new HashMap<String, ResourceWeights>();
    queueMaxApps = new HashMap<String, Integer>();
    userMaxApps = new HashMap<String, Integer>();
    userMaxAppsDefault = Integer.MAX_VALUE;
    queueMaxAppsDefault = Integer.MAX_VALUE;
    queueAcls = new HashMap<String, Map<QueueACL, AccessControlList>>();
    minSharePreemptionTimeouts = new HashMap<String, Long>();
    defaultMinSharePreemptionTimeout = Long.MAX_VALUE;
    fairSharePreemptionTimeout = Long.MAX_VALUE;
    schedulingPolicies = new HashMap<String, SchedulingPolicy>();
    defaultSchedulingPolicy = SchedulingPolicy.DEFAULT_POLICY;
    placementPolicy = QueuePlacementPolicy.fromConfiguration(conf,
        new HashSet<String>());
    queueNames = new HashSet<String>();
  }
  
  /**
   * Get the ACLs associated with this queue. If a given ACL is not explicitly
   * configured, include the default value for that ACL.  The default for the
   * root queue is everybody ("*") and the default for all other queues is
   * nobody ("")
   */
  public AccessControlList getQueueAcl(String queue, QueueACL operation) {
    Map<QueueACL, AccessControlList> queueAcls = this.queueAcls.get(queue);
    if (queueAcls != null) {
      AccessControlList operationAcl = queueAcls.get(operation);
      if (operationAcl != null) {
        return operationAcl;
      }
    }
    return (queue.equals("root")) ? EVERYBODY_ACL : NOBODY_ACL;
  }
  
  /**
   * Get a queue's min share preemption timeout, in milliseconds. This is the
   * time after which jobs in the queue may kill other queues' tasks if they
   * are below their min share.
   */
  public long getMinSharePreemptionTimeout(String queueName) {
    Long minSharePreemptionTimeout = minSharePreemptionTimeouts.get(queueName);
    return (minSharePreemptionTimeout == null) ? defaultMinSharePreemptionTimeout
        : minSharePreemptionTimeout;
  }
  
  /**
   * Get the fair share preemption, in milliseconds. This is the time
   * after which any job may kill other jobs' tasks if it is below half
   * its fair share.
   */
  public long getFairSharePreemptionTimeout() {
    return fairSharePreemptionTimeout;
  }
  
  public ResourceWeights getQueueWeight(String queue) {
    ResourceWeights weight = queueWeights.get(queue);
    return (weight == null) ? ResourceWeights.NEUTRAL : weight;
  }
  
  public int getUserMaxApps(String user) {
    Integer maxApps = userMaxApps.get(user);
    return (maxApps == null) ? userMaxAppsDefault : maxApps;
  }

  public int getQueueMaxApps(String queue) {
    Integer maxApps = queueMaxApps.get(queue);
    return (maxApps == null) ? queueMaxAppsDefault : maxApps;
  }
  
  /**
   * Get the minimum resource allocation for the given queue.
   * @return the cap set on this queue, or 0 if not set.
   */
  public Resource getMinResources(String queue) {
    Resource minQueueResource = minQueueResources.get(queue);
    return (minQueueResource == null) ? Resources.none() : minQueueResource;
  }

  /**
   * Get the maximum resource allocation for the given queue.
   * @return the cap set on this queue, or Integer.MAX_VALUE if not set.
   */

  public Resource getMaxResources(String queueName) {
    Resource maxQueueResource = maxQueueResources.get(queueName);
    return (maxQueueResource == null) ? Resources.unbounded() : maxQueueResource;
  }
  
  public boolean hasAccess(String queueName, QueueACL acl,
      UserGroupInformation user) {
    int lastPeriodIndex = queueName.length();
    while (lastPeriodIndex != -1) {
      String queue = queueName.substring(0, lastPeriodIndex);
      if (getQueueAcl(queue, acl).isUserAllowed(user)) {
        return true;
      }

      lastPeriodIndex = queueName.lastIndexOf('.', lastPeriodIndex - 1);
    }
    
    return false;
  }
  
  public SchedulingPolicy getSchedulingPolicy(String queueName) {
    SchedulingPolicy policy = schedulingPolicies.get(queueName);
    return (policy == null) ? defaultSchedulingPolicy : policy;
  }
  
  public SchedulingPolicy getDefaultSchedulingPolicy() {
    return defaultSchedulingPolicy;
  }
  
  public Set<String> getQueueNames() {
    return queueNames;
  }
  
  public QueuePlacementPolicy getPlacementPolicy() {
    return placementPolicy;
  }
}