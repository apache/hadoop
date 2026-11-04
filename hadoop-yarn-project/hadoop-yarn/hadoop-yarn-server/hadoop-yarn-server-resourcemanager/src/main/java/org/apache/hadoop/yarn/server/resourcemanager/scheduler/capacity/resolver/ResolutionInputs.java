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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
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
    this.dynamicQueues = Collections.unmodifiableList(
        new ArrayList<>(dynamicQueues));
    this.clusterMaximumAllocation = Resources.clone(clusterMaximumAllocation);
    this.clusterMaximumApplicationPriority = clusterMaximumApplicationPriority;
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
}
