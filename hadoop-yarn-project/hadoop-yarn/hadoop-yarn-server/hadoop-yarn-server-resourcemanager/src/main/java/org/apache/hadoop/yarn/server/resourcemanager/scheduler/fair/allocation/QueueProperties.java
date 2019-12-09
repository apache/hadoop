/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation;

import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.ConfigurableResource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueueType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class is a value class, storing queue properties parsed from the
 * allocation.xml config file. Since there are a bunch of properties, properties
 * should be added via QueueProperties.Builder.
 */
public class QueueProperties {
  // Create some temporary hashmaps to hold the new allocs, and we only save
  // them in our fields if we have parsed the entire allocations file
  // successfully.
  private final Map<String, Resource> minQueueResources;
  private final Map<String, ConfigurableResource> maxQueueResources;
  private final Map<String, ConfigurableResource> maxChildQueueResources;
  private final Map<String, Integer> queueMaxApps;
  private final Map<String, Float> queueMaxAMShares;
  private final Map<String, Float> queueWeights;
  private final Map<String, SchedulingPolicy> queuePolicies;
  private final Map<String, Long> minSharePreemptionTimeouts;
  private final Map<String, Long> fairSharePreemptionTimeouts;
  private final Map<String, Float> fairSharePreemptionThresholds;
  private final Map<String, Map<AccessType, AccessControlList>> queueAcls;
  private final Map<String, Map<ReservationACL, AccessControlList>>
          reservationAcls;
  private final Set<String> reservableQueues;
  private final Set<String> nonPreemptableQueues;
  private final Map<FSQueueType, Set<String>> configuredQueues;
  private final Map<String, Resource> queueMaxContainerAllocation;

  QueueProperties(Builder builder) {
    this.reservableQueues = builder.reservableQueues;
    this.minQueueResources = builder.minQueueResources;
    this.fairSharePreemptionTimeouts = builder.fairSharePreemptionTimeouts;
    this.queueWeights = builder.queueWeights;
    this.nonPreemptableQueues = builder.nonPreemptableQueues;
    this.configuredQueues = builder.configuredQueues;
    this.queueMaxAMShares = builder.queueMaxAMShares;
    this.queuePolicies = builder.queuePolicies;
    this.fairSharePreemptionThresholds = builder.fairSharePreemptionThresholds;
    this.queueMaxApps = builder.queueMaxApps;
    this.minSharePreemptionTimeouts = builder.minSharePreemptionTimeouts;
    this.maxQueueResources = builder.maxQueueResources;
    this.maxChildQueueResources = builder.maxChildQueueResources;
    this.reservationAcls = builder.reservationAcls;
    this.queueAcls = builder.queueAcls;
    this.queueMaxContainerAllocation = builder.queueMaxContainerAllocation;
  }

  public Map<FSQueueType, Set<String>> getConfiguredQueues() {
    return configuredQueues;
  }

  public Map<String, Long> getMinSharePreemptionTimeouts() {
    return minSharePreemptionTimeouts;
  }

  public Map<String, Long> getFairSharePreemptionTimeouts() {
    return fairSharePreemptionTimeouts;
  }

  public Map<String, Float> getFairSharePreemptionThresholds() {
    return fairSharePreemptionThresholds;
  }

  public Map<String, Resource> getMinQueueResources() {
    return minQueueResources;
  }

  public Map<String, ConfigurableResource> getMaxQueueResources() {
    return maxQueueResources;
  }

  public Map<String, ConfigurableResource> getMaxChildQueueResources() {
    return maxChildQueueResources;
  }

  public Map<String, Integer> getQueueMaxApps() {
    return queueMaxApps;
  }

  public Map<String, Float> getQueueWeights() {
    return queueWeights;
  }

  public Map<String, Float> getQueueMaxAMShares() {
    return queueMaxAMShares;
  }

  public Map<String, SchedulingPolicy> getQueuePolicies() {
    return queuePolicies;
  }

  public Map<String, Map<AccessType, AccessControlList>> getQueueAcls() {
    return queueAcls;
  }

  public Map<String, Map<ReservationACL, AccessControlList>>
      getReservationAcls() {
    return reservationAcls;
  }

  public Set<String> getReservableQueues() {
    return reservableQueues;
  }

  public Set<String> getNonPreemptableQueues() {
    return nonPreemptableQueues;
  }

  public Map<String, Resource> getMaxContainerAllocation() {
    return queueMaxContainerAllocation;
  }

    /**
   * Builder class for {@link QueueProperties}.
   * All methods are adding queue properties to the maps of this builder
   * keyed by the queue's name except some methods
   * like {@link #isAclDefinedForAccessType(String, AccessType)} or
   * {@link #getMinQueueResources()}.
   *
   */
  public static final class Builder {
    private Map<String, Resource> minQueueResources = new HashMap<>();
    private Map<String, ConfigurableResource> maxQueueResources =
        new HashMap<>();
    private Map<String, ConfigurableResource> maxChildQueueResources =
        new HashMap<>();
    private Map<String, Integer> queueMaxApps = new HashMap<>();
    private Map<String, Float> queueMaxAMShares = new HashMap<>();
    private Map<String, Resource> queueMaxContainerAllocation = new HashMap<>();
    private Map<String, Float> queueWeights = new HashMap<>();
    private Map<String, SchedulingPolicy> queuePolicies = new HashMap<>();
    private Map<String, Long> minSharePreemptionTimeouts = new HashMap<>();
    private Map<String, Long> fairSharePreemptionTimeouts = new HashMap<>();
    private Map<String, Float> fairSharePreemptionThresholds = new HashMap<>();
    private Map<String, Map<AccessType, AccessControlList>> queueAcls =
        new HashMap<>();
    private Map<String, Map<ReservationACL, AccessControlList>>
            reservationAcls = new HashMap<>();
    private Set<String> reservableQueues = new HashSet<>();
    private Set<String> nonPreemptableQueues = new HashSet<>();
    // Remember all queue names so we can display them on web UI, etc.
    // configuredQueues is segregated based on whether it is a leaf queue
    // or a parent queue. This information is used for creating queues
    // and also for making queue placement decisions(QueuePlacementRule.java).
    private Map<FSQueueType, Set<String>> configuredQueues = new HashMap<>();

    Builder() {
      for (FSQueueType queueType : FSQueueType.values()) {
        configuredQueues.put(queueType, new HashSet<>());
      }
    }

    public static Builder create() {
      return new Builder();
    }

    public Builder minQueueResources(String queueName, Resource resource) {
      this.minQueueResources.put(queueName, resource);
      return this;
    }

    public Builder maxQueueResources(String queueName,
        ConfigurableResource resource) {
      this.maxQueueResources.put(queueName, resource);
      return this;
    }

    public Builder maxChildQueueResources(String queueName,
        ConfigurableResource resource) {
      this.maxChildQueueResources.put(queueName, resource);
      return this;
    }

    public Builder queueMaxApps(String queueName, int value) {
      this.queueMaxApps.put(queueName, value);
      return this;
    }

    public Builder queueMaxAMShares(String queueName, float value) {
      this.queueMaxAMShares.put(queueName, value);
      return this;
    }

    public Builder queueWeights(String queueName, float value) {
      this.queueWeights.put(queueName, value);
      return this;
    }

    public Builder queuePolicies(String queueName, SchedulingPolicy policy) {
      this.queuePolicies.put(queueName, policy);
      return this;
    }

    public Builder minSharePreemptionTimeouts(String queueName, long value) {
      this.minSharePreemptionTimeouts.put(queueName, value);
      return this;
    }

    public Builder fairSharePreemptionTimeouts(String queueName, long value) {
      this.fairSharePreemptionTimeouts.put(queueName, value);
      return this;
    }

    public Builder fairSharePreemptionThresholds(String queueName,
        float value) {
      this.fairSharePreemptionThresholds.put(queueName, value);
      return this;
    }

    public Builder queueAcls(String queueName, AccessType accessType,
        AccessControlList acls) {
      this.queueAcls.putIfAbsent(queueName, new HashMap<>());
      this.queueAcls.get(queueName).put(accessType, acls);
      return this;
    }

    public Builder reservationAcls(String queueName,
        ReservationACL reservationACL, AccessControlList acls) {
      this.reservationAcls.putIfAbsent(queueName, new HashMap<>());
      this.reservationAcls.get(queueName).put(reservationACL, acls);
      return this;
    }

    public Builder reservableQueues(String queue) {
      this.reservableQueues.add(queue);
      return this;
    }

    public Builder nonPreemptableQueues(String queue) {
      this.nonPreemptableQueues.add(queue);
      return this;
    }

    public Builder queueMaxContainerAllocation(String queueName,
        Resource value) {
      queueMaxContainerAllocation.put(queueName, value);
      return this;
    }

    public void configuredQueues(FSQueueType queueType, String queueName) {
      this.configuredQueues.get(queueType).add(queueName);
    }

    public boolean isAclDefinedForAccessType(String queueName,
        AccessType accessType) {
      Map<AccessType, AccessControlList> aclsForQueue =
          this.queueAcls.get(queueName);
      return aclsForQueue != null && aclsForQueue.get(accessType) != null;
    }

    public Map<String, Resource> getMinQueueResources() {
      return minQueueResources;
    }

    public Map<String, ConfigurableResource> getMaxQueueResources() {
      return maxQueueResources;
    }

    public QueueProperties build() {
      return new QueueProperties(this);
    }
  }
}
