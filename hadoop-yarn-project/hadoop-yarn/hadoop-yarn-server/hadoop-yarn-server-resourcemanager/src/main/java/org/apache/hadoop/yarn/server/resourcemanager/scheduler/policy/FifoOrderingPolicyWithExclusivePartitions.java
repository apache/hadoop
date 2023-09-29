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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;


/**
 * Similar to {@link FifoOrderingPolicy}, but with separate ordering policies
 * for each partition in
 * {@code yarn.scheduler.capacity.<queue-path>.ordering-policy.exclusive-enforced-partitions}.
 */
public class FifoOrderingPolicyWithExclusivePartitions<S extends SchedulableEntity>
    implements OrderingPolicy<S> {

  private static final String DEFAULT_PARTITION = "DEFAULT_PARTITION";

  private Map<String, OrderingPolicy<S>> orderingPolicies;

  public FifoOrderingPolicyWithExclusivePartitions() {
    this.orderingPolicies = new HashMap<>();
    this.orderingPolicies.put(DEFAULT_PARTITION, new FifoOrderingPolicy());
  }

  public Collection<S> getSchedulableEntities() {
    return unionOrderingPolicies().getSchedulableEntities();
  }

  public Iterator<S> getAssignmentIterator(IteratorSelector sel) {
    // Return schedulable entities only from filtered partition
    return getPartitionOrderingPolicy(sel.getPartition())
        .getAssignmentIterator(sel);
  }

  public Iterator<S> getPreemptionIterator() {
    // Entities from all partitions should be preemptible
    return unionOrderingPolicies().getPreemptionIterator();
  }

  /**
   * Union all schedulable entities from all ordering policies.
   * @return ordering policy containing all schedulable entities
   */
  private OrderingPolicy<S> unionOrderingPolicies() {
    OrderingPolicy<S> ret = new FifoOrderingPolicy<>();
    for (Map.Entry<String, OrderingPolicy<S>> entry
        : orderingPolicies.entrySet()) {
      ret.addAllSchedulableEntities(entry.getValue().getSchedulableEntities());
    }
    return ret;
  }

  public void addSchedulableEntity(S s) {
    getPartitionOrderingPolicy(s.getPartition()).addSchedulableEntity(s);
  }

  public boolean removeSchedulableEntity(S s) {
    return getPartitionOrderingPolicy(s.getPartition())
        .removeSchedulableEntity(s);
  }

  public void addAllSchedulableEntities(Collection<S> sc) {
    for (S entity : sc) {
      getPartitionOrderingPolicy(entity.getPartition())
          .addSchedulableEntity(entity);
    }
  }

  public int getNumSchedulableEntities() {
    // Return total number of schedulable entities, to maintain parity with
    // existing FifoOrderingPolicy e.g. when determining if queue has reached
    // its max app limit
    int ret = 0;
    for (Map.Entry<String, OrderingPolicy<S>> entry
        : orderingPolicies.entrySet()) {
      ret += entry.getValue().getNumSchedulableEntities();
    }
    return ret;
  }

  public void containerAllocated(S schedulableEntity, RMContainer r) {
    getPartitionOrderingPolicy(schedulableEntity.getPartition())
        .containerAllocated(schedulableEntity, r);
  }

  public void containerReleased(S schedulableEntity, RMContainer r) {
    getPartitionOrderingPolicy(schedulableEntity.getPartition())
        .containerReleased(schedulableEntity, r);
  }

  public void demandUpdated(S schedulableEntity) {
    getPartitionOrderingPolicy(schedulableEntity.getPartition())
        .demandUpdated(schedulableEntity);
  }

  @Override
  public void configure(Map<String, String> conf) {
    if (conf == null) {
      return;
    }
    String partitions =
        conf.get(YarnConfiguration.EXCLUSIVE_ENFORCED_PARTITIONS_SUFFIX);
    if (partitions != null) {
      for (String partition : partitions.split(",")) {
        partition = partition.trim();
        if (!partition.isEmpty()) {
          this.orderingPolicies.put(partition, new FifoOrderingPolicy());
        }
      }
    }
  }

  @Override
  public String getInfo() {
    return "FifoOrderingPolicyWithExclusivePartitions";
  }

  @Override
  public String getConfigName() {
    return CapacitySchedulerConfiguration
        .FIFO_WITH_PARTITIONS_APP_ORDERING_POLICY;
  }

  private OrderingPolicy<S> getPartitionOrderingPolicy(String partition) {
    String keyPartition = orderingPolicies.containsKey(partition) ?
        partition : DEFAULT_PARTITION;
    return orderingPolicies.get(keyPartition);
  }
}
