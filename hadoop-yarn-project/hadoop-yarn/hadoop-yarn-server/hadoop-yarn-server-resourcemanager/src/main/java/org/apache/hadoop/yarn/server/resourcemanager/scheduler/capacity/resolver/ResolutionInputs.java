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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * What queue configuration resolution needs besides the configuration:
 * state held by the scheduler.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ResolutionInputs {

  /** A dynamic queue that exists in the scheduler. */
  public static final class DynamicQueue {
    private final QueuePath path;
    private final boolean legacyAutoCreated;
    private final boolean leaf;

    /**
     * @param path the queue path
     * @param legacyAutoCreated true for a v1 auto-created leaf of a managed
     *                          parent, false for a v2 dynamic queue
     * @param leaf true for a leaf, false for a parent
     */
    public DynamicQueue(QueuePath path, boolean legacyAutoCreated,
        boolean leaf) {
      this.path = path;
      this.legacyAutoCreated = legacyAutoCreated;
      this.leaf = leaf;
    }

    /**
     * Classifies an existing queue: an AQC v1 auto-created leaf
     * ({@code AutoCreatedLeafQueue}) receives the leaf queue template of its
     * managed parent, any other queue marked dynamic is an AQC v2 queue.
     * @param path the queue path
     * @param legacyAutoCreated whether the queue is an AQC v1 auto-created leaf
     * @param dynamic whether the queue is marked dynamic
     * @param leaf whether the queue is a leaf
     * @return the dynamic queue, or null for a static queue
     */
    public static DynamicQueue of(QueuePath path, boolean legacyAutoCreated,
        boolean dynamic, boolean leaf) {
      if (legacyAutoCreated) {
        return new DynamicQueue(path, true, true);
      }
      return dynamic ? new DynamicQueue(path, false, leaf) : null;
    }

    public QueuePath getPath() {
      return path;
    }

    public boolean isLegacyAutoCreated() {
      return legacyAutoCreated;
    }

    public boolean isLeaf() {
      return leaf;
    }
  }

  private final List<DynamicQueue> dynamicQueues;
  private final Resource clusterMaximumAllocation;
  private final Priority clusterMaximumApplicationPriority;
  private final RuntimeException clusterMaximumAllocationFailure;
  private final RuntimeException clusterMaximumApplicationPriorityFailure;

  /**
   * @param dynamicQueues the existing dynamic queues
   * @param clusterMaximumAllocation the cluster maximum allocation, the
   *                                 baseline of the root queue's maximum
   *                                 allocation
   * @param clusterMaximumApplicationPriority the cap of the priority ACLs
   */
  public ResolutionInputs(List<DynamicQueue> dynamicQueues,
      Resource clusterMaximumAllocation,
      Priority clusterMaximumApplicationPriority) {
    this(dynamicQueues, clusterMaximumAllocation,
        clusterMaximumApplicationPriority, null, null);
  }

  private ResolutionInputs(List<DynamicQueue> dynamicQueues,
      Resource clusterMaximumAllocation,
      Priority clusterMaximumApplicationPriority,
      RuntimeException clusterMaximumAllocationFailure,
      RuntimeException clusterMaximumApplicationPriorityFailure) {
    this.dynamicQueues = Collections.unmodifiableList(
        new ArrayList<>(dynamicQueues));
    this.clusterMaximumAllocation = Resources.clone(clusterMaximumAllocation);
    this.clusterMaximumApplicationPriority = clusterMaximumApplicationPriority;
    this.clusterMaximumAllocationFailure = clusterMaximumAllocationFailure;
    this.clusterMaximumApplicationPriorityFailure =
        clusterMaximumApplicationPriorityFailure;
  }

  /**
   * Reads the scheduler-wide inputs from a configuration. Queue setup reads
   * the cluster maximum allocation and priority itself and fails there if
   * they cannot be read, so a value that cannot be read is replaced by zero
   * and its failure is kept for validation.
   * @param conf the configuration
   * @param dynamicQueues the existing dynamic queues
   * @return the inputs
   */
  public static ResolutionInputs from(CapacitySchedulerConfiguration conf,
      List<DynamicQueue> dynamicQueues) {
    Resource maximumAllocation = Resources.none();
    RuntimeException maximumAllocationFailure = null;
    try {
      maximumAllocation = ResourceUtils.fetchMaximumAllocationFromConfig(conf);
    } catch (RuntimeException e) {
      maximumAllocationFailure = e;
    }
    Priority maximumPriority = Priority.newInstance(0);
    RuntimeException maximumPriorityFailure = null;
    try {
      maximumPriority = conf.getClusterLevelApplicationMaxPriority();
    } catch (RuntimeException e) {
      maximumPriorityFailure = e;
    }
    return new ResolutionInputs(dynamicQueues, maximumAllocation,
        maximumPriority, maximumAllocationFailure, maximumPriorityFailure);
  }

  public List<DynamicQueue> getDynamicQueues() {
    return dynamicQueues;
  }

  public Resource getClusterMaximumAllocation() {
    return clusterMaximumAllocation;
  }

  public Priority getClusterMaximumApplicationPriority() {
    return clusterMaximumApplicationPriority;
  }

  /**
   * @return why {@link #from} could not read the cluster maximum allocation,
   *         or null
   */
  public RuntimeException getClusterMaximumAllocationFailure() {
    return clusterMaximumAllocationFailure;
  }

  /**
   * @return why {@link #from} could not read the cluster maximum application
   *         priority, or null
   */
  public RuntimeException getClusterMaximumApplicationPriorityFailure() {
    return clusterMaximumApplicationPriorityFailure;
  }
}
