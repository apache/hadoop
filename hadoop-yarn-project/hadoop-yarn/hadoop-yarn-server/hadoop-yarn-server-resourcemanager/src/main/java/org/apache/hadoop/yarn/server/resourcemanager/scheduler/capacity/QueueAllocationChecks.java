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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.UNDEFINED;

/**
 * Scheduler and queue allocation checks, shared by scheduler and queue
 * initialization and configuration validation. Each check returns null when it
 * passes, or the error message the initialization fails with.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class QueueAllocationChecks {

  private QueueAllocationChecks() {
  }

  /**
   * Checks the scheduler minimum and maximum memory allocation. The exception
   * type of a failure is
   * {@link org.apache.hadoop.yarn.exceptions.YarnRuntimeException}.
   * @param memory configured minimum and maximum allocation in MB
   * @return null if the range is valid, the error message otherwise
   */
  public static String checkMemoryAllocation(AllocationRange memory) {
    int minMem = memory.getMinimum();
    int maxMem = memory.getMaximum();
    if (minMem <= 0 || minMem > maxMem) {
      return "Invalid resource scheduler memory"
          + " allocation configuration"
          + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
          + "=" + minMem
          + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
          + "=" + maxMem + ", min and max should be greater than 0"
          + ", max should be no smaller than min.";
    }
    return null;
  }

  /**
   * Checks the scheduler minimum and maximum vcores allocation. The exception
   * type of a failure is
   * {@link org.apache.hadoop.yarn.exceptions.YarnRuntimeException}.
   * @param vcores configured minimum and maximum allocation in vcores
   * @return null if the range is valid, the error message otherwise
   */
  public static String checkVcoresAllocation(AllocationRange vcores) {
    int minVcores = vcores.getMinimum();
    int maxVcores = vcores.getMaximum();
    if (minVcores <= 0 || minVcores > maxVcores) {
      return "Invalid resource scheduler vcores"
          + " allocation configuration"
          + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
          + "=" + minVcores
          + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES
          + "=" + maxVcores + ", min and max should be greater than 0"
          + ", max should be no smaller than min.";
    }
    return null;
  }

  /**
   * Checks the legacy per queue {@code maximum-allocation-mb} and
   * {@code maximum-allocation-vcores} against the cluster maximum allocation.
   * The parent's maximum allocation is not checked. The exception type of a
   * failure is {@link IllegalArgumentException}.
   * @param input the legacy maximum allocation of the queue
   * @return null if the allocation fits, the error message otherwise
   */
  public static String checkLegacyQueueMaximumAllocation(
      LegacyMaximumAllocationInput input) {
    long queueMemory = input.getQueueMemory();
    int queueVcores = input.getQueueVcores();
    Resource clusterMax = input.getClusterMaximumAllocation();
    if ((queueMemory != UNDEFINED && queueMemory > clusterMax.getMemorySize()
        || (queueVcores != UNDEFINED
        && queueVcores > clusterMax.getVirtualCores()))) {
      return String.format(maximumAllocationMessage(input.getQueuePath(),
          clusterMax), input.getEffectiveMaximumAllocation());
    }
    return null;
  }

  /**
   * Checks every resource of the per queue {@code maximum-allocation} against
   * the cluster maximum allocation. The parent's maximum allocation is not
   * checked. The exception type of a failure is
   * {@link IllegalArgumentException}; a resource unknown to the cluster
   * maximum allocation throws the same exception as the queue initialization.
   * @param input the maximum allocation of the queue
   * @return null if the allocation fits, the error message otherwise
   */
  public static String checkQueueMaximumAllocation(
      MaximumAllocationInput input) {
    Resource queueMax = input.getQueueMaximumAllocation();
    Resource clusterMax = input.getClusterMaximumAllocation();
    for (ResourceInformation ri : queueMax.getResources()) {
      if (ri.compareTo(clusterMax.getResourceInformation(ri.getName())) > 0) {
        return String.format(maximumAllocationMessage(input.getQueuePath(),
            clusterMax), queueMax);
      }
    }
    return null;
  }

  /**
   * Checks that reinitializing a leaf queue does not decrease its maximum
   * allocation, since running AMs were already told the size. The exception
   * type of a failure is {@link java.io.IOException}.
   * @param queuePath the full path of the leaf queue
   * @param currentMaximum the maximum allocation of the live queue
   * @param newMaximum the maximum allocation of the newly parsed queue
   * @return null if the new maximum fits, the error message otherwise
   */
  public static String checkMaximumAllocationNotDecreased(String queuePath,
      Resource currentMaximum, Resource newMaximum) {
    if (!Resources.fitsIn(currentMaximum, newMaximum)) {
      return "Trying to reinitialize " + queuePath
          + " the maximum allocation size can not be decreased!"
          + " Current setting: " + currentMaximum + ", trying to set it to: "
          + newMaximum;
    }
    return null;
  }

  // The queue path and the cluster maximum are part of the format string,
  // as they always were, so they are kept out of the format arguments.
  private static String maximumAllocationMessage(String queuePath,
      Resource clusterMax) {
    return "Queue maximum allocation cannot be larger than the cluster setting"
        + " for queue " + queuePath
        + " max allocation per queue: %s"
        + " cluster setting: " + clusterMax;
  }

  // The Resource values of the inputs below are not copied, because a copy
  // can change units and resource type layout; callers pass values they no
  // longer modify.

  /**
   * A configured minimum and maximum allocation of one resource.
   */
  public static final class AllocationRange {
    private final int minimum;
    private final int maximum;

    public AllocationRange(int minimum, int maximum) {
      this.minimum = minimum;
      this.maximum = maximum;
    }

    public int getMinimum() {
      return minimum;
    }

    public int getMaximum() {
      return maximum;
    }
  }

  /**
   * Input of
   * {@link #checkLegacyQueueMaximumAllocation(LegacyMaximumAllocationInput)}.
   */
  public static final class LegacyMaximumAllocationInput {
    private final String queuePath;
    private final long queueMemory;
    private final int queueVcores;
    private final Resource clusterMaximumAllocation;
    private final Resource effectiveMaximumAllocation;

    /**
     * @param queuePath full path of the queue, used in the message
     * @param queueMemory configured {@code maximum-allocation-mb}, or
     *                    {@code UNDEFINED}
     * @param queueVcores configured {@code maximum-allocation-vcores}, or
     *                    {@code UNDEFINED}
     * @param clusterMaximumAllocation cluster maximum allocation
     * @param effectiveMaximumAllocation the queue's maximum allocation (the
     *                                   parent's, or the cluster's for root,
     *                                   overridden by the configured values),
     *                                   used in the message only
     */
    public LegacyMaximumAllocationInput(String queuePath, long queueMemory,
        int queueVcores, Resource clusterMaximumAllocation,
        Resource effectiveMaximumAllocation) {
      this.queuePath = queuePath;
      this.queueMemory = queueMemory;
      this.queueVcores = queueVcores;
      this.clusterMaximumAllocation = clusterMaximumAllocation;
      this.effectiveMaximumAllocation = effectiveMaximumAllocation;
    }

    public String getQueuePath() {
      return queuePath;
    }

    public long getQueueMemory() {
      return queueMemory;
    }

    public int getQueueVcores() {
      return queueVcores;
    }

    public Resource getClusterMaximumAllocation() {
      return clusterMaximumAllocation;
    }

    public Resource getEffectiveMaximumAllocation() {
      return effectiveMaximumAllocation;
    }
  }

  /**
   * Input of {@link #checkQueueMaximumAllocation(MaximumAllocationInput)}.
   */
  public static final class MaximumAllocationInput {
    private final String queuePath;
    private final Resource queueMaximumAllocation;
    private final Resource clusterMaximumAllocation;

    /**
     * @param queuePath full path of the queue, used in the message
     * @param queueMaximumAllocation configured {@code maximum-allocation}
     * @param clusterMaximumAllocation cluster maximum allocation
     */
    public MaximumAllocationInput(String queuePath,
        Resource queueMaximumAllocation, Resource clusterMaximumAllocation) {
      this.queuePath = queuePath;
      this.queueMaximumAllocation = queueMaximumAllocation;
      this.clusterMaximumAllocation = clusterMaximumAllocation;
    }

    public String getQueuePath() {
      return queuePath;
    }

    public Resource getQueueMaximumAllocation() {
      return queueMaximumAllocation;
    }

    public Resource getClusterMaximumAllocation() {
      return clusterMaximumAllocation;
    }
  }
}
