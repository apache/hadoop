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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigValidator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueAllocationChecks;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MAXIMUM_ALLOCATION;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MAXIMUM_ALLOCATION_VCORES;

/**
 * Scheduler and queue allocation checks ({@link QueueAllocationChecks}):
 * the scheduler minimum and maximum allocation, the queue maximum allocation
 * against the cluster maximum, and the maximum allocation of a live leaf,
 * which a refresh cannot decrease.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class AllocationRule implements ValidationRule {
  /** A01, scheduler minimum and maximum memory allocation. */
  public static final String INVALID_MEMORY_ALLOCATION =
      "invalid-memory-allocation";
  /** A02, scheduler minimum and maximum vcores allocation. */
  public static final String INVALID_VCORES_ALLOCATION =
      "invalid-vcores-allocation";
  /** A05, malformed queue or scheduler maximum allocation. */
  public static final String INVALID_MAXIMUM_ALLOCATION =
      "invalid-maximum-allocation";
  /** A03 and A04, queue maximum allocation above the cluster maximum. */
  public static final String MAXIMUM_ALLOCATION_EXCEEDS_CLUSTER =
      "maximum-allocation-exceeds-cluster";
  /** A06, decreased maximum allocation of a live leaf queue. */
  public static final String MAXIMUM_ALLOCATION_DECREASED =
      "maximum-allocation-decreased";

  @Override
  public String getId() {
    return "allocation";
  }

  @Override
  public void check(ValidationContext context, List<ValidationIssue> issues) {
    CapacitySchedulerConfiguration conf = context.getConfiguration();
    try {
      CapacitySchedulerConfigValidator.validateMemoryAllocation(conf);
    } catch (YarnRuntimeException | NumberFormatException e) {
      issues.add(ValidationIssue.error(null,
          YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
          INVALID_MEMORY_ALLOCATION, e));
    }
    try {
      CapacitySchedulerConfigValidator.validateVCores(conf);
    } catch (YarnRuntimeException | NumberFormatException e) {
      issues.add(ValidationIssue.error(null,
          YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
          INVALID_VCORES_ALLOCATION, e));
    }

    Resource clusterMax = context.getClusterMaximumAllocation();
    if (clusterMax == null) {
      issues.add(ValidationIssue.error(null,
          YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
          INVALID_MAXIMUM_ALLOCATION,
          context.getClusterMaximumAllocationFailure()));
      return;
    }
    for (ResolvedQueueConfig queue : context.getTree().getQueues()) {
      checkQueue(context, queue, clusterMax, issues);
    }
  }

  private static void checkQueue(ValidationContext context,
      ResolvedQueueConfig queue, Resource clusterMax,
      List<ValidationIssue> issues) {
    String path = queue.getQueuePath().getFullPath();
    Resolved<Resource> maximum = queue.get(MAXIMUM_ALLOCATION);
    if (maximum == null) {
      return;
    }
    if (maximum.isFailed()) {
      RuleSupport.reportFailure(issues, queue, MAXIMUM_ALLOCATION, "",
          INVALID_MAXIMUM_ALLOCATION, ValidationIssue.Severity.ERROR);
      return;
    }

    String error;
    String key;
    String detail = maximum.getSourceDetail();
    if (detail != null && detail.endsWith(
        "." + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION)) {
      // maximum-allocation is set; it replaces every resource of the base
      key = detail;
      error = QueueAllocationChecks.checkQueueMaximumAllocation(
          path, maximum.getValue(), clusterMax);
    } else {
      Resolved<Long> memory = queue.get(MAXIMUM_ALLOCATION_MB);
      Resolved<Integer> vcores = queue.get(MAXIMUM_ALLOCATION_VCORES);
      if (!RuleSupport.ok(memory) || !RuleSupport.ok(vcores)) {
        return;
      }
      key = RuleSupport.key(queue, memory.getValue()
          != CapacitySchedulerConfiguration.UNDEFINED ? MAXIMUM_ALLOCATION_MB
          : MAXIMUM_ALLOCATION_VCORES, "");
      error = QueueAllocationChecks.checkLegacyQueueMaximumAllocation(
          path, memory.getValue(), vcores.getValue(), clusterMax,
          maximum.getValue());
    }
    if (error != null) {
      issues.add(ValidationIssue.error(path, key,
          MAXIMUM_ALLOCATION_EXCEEDS_CLUSTER, error));
      return;
    }

    // A live leaf that stays a leaf is reinitialized from the new queue
    ClusterFacts.QueueFacts live = context.getFacts().getQueue(path);
    if (live != null && live.getKind() == QueueKind.LEAF && !live.isDynamic()
        && !live.isAutoCreatedLeaf() && queue.getKind() == Kind.LEAF
        && live.getMaximumAllocation() != null) {
      error = QueueAllocationChecks.checkMaximumAllocationNotDecreased(path,
          live.getMaximumAllocation(), maximum.getValue());
      if (error != null) {
        issues.add(ValidationIssue.error(path,
            RuleSupport.key(queue, MAXIMUM_ALLOCATION, ""),
            MAXIMUM_ALLOCATION_DECREASED, error));
      }
    }
  }
}
