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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * A Schedulable represents an entity that can be scheduled such as an
 * application or a queue. It provides a common interface so that algorithms
 * such as fair sharing can be applied both within a queue and across queues.
 *
 * A Schedulable is responsible for three roles:
 * 1) Assign resources through {@link #assignContainer}.
 * 2) It provides information about the app/queue to the scheduler, including:
 *    - Demand (maximum number of tasks required)
 *    - Minimum share (for queues)
 *    - Job/queue weight (for fair sharing)
 *    - Start time and priority (for FIFO)
 * 3) It can be assigned a fair share, for use with fair scheduling.
 *
 * Schedulable also contains two methods for performing scheduling computations:
 * - updateDemand() is called periodically to compute the demand of the various
 *   jobs and queues, which may be expensive (e.g. jobs must iterate through all
 *   their tasks to count failed tasks, tasks that can be speculated, etc).
 */
@Private
@Unstable
public interface Schedulable {
  /**
   * Name of job/queue, used for debugging as well as for breaking ties in
   * scheduling order deterministically.
   * @return Name of job/queue.
   */
  String getName();

  /**
   * Maximum number of resources required by this Schedulable. This is defined as
   * number of currently utilized resources + number of unlaunched resources (that
   * are either not yet launched or need to be speculated).
   * @return resources required by this Schedulable.
   */
  Resource getDemand();

  /**
   * Get the aggregate amount of resources consumed by the schedulable.
   * @return aggregate amount of resources.
   */
  Resource getResourceUsage();

  /**
   * Minimum Resource share assigned to the schedulable.
   * @return Minimum Resource share.
   */
  Resource getMinShare();

  /**
   * Maximum Resource share assigned to the schedulable.
   * @return Maximum Resource share.
   */
  Resource getMaxShare();

  /** The ratio of Maximum Resource share that can be assigned to the schedulable. */
  float getMaxAppShare();

  /**
   * Job/queue weight in fair sharing. Weights are only meaningful when
   * compared. A weight of 2.0f has twice the weight of a weight of 1.0f,
   * which has twice the weight of a weight of 0.5f. A weight of 1.0f is
   * considered unweighted or a neutral weight. A weight of 0 is no weight.
   *
   * @return the weight
   */
  float getWeight();

  /**
   * Start time for jobs in FIFO queues; meaningless for QueueSchedulables.
   * @return Start time for jobs.
   */
  long getStartTime();

 /**
  * Job priority for jobs in FIFO queues; meaningless for QueueSchedulables.
  * @return Job priority.
  */
  Priority getPriority();

  /** Refresh the Schedulable's demand and those of its children if any. */
  void updateDemand();

  /**
   * Assign a container on this node if possible, and return the amount of
   * resources assigned.
   *
   * @param node FSSchedulerNode.
   * @return the amount of resources assigned.
   */
  Resource assignContainer(FSSchedulerNode node);

  /**
   * Get the fair share assigned to this Schedulable.
   * @return the fair share assigned to this Schedulable.
   */
  Resource getFairShare();

  /**
   * Assign a fair share to this Schedulable.
   * @param fairShare a fair share to this Schedulable.
   */
  void setFairShare(Resource fairShare);

  /**
   * Check whether the schedulable is preemptable.
   * @return <code>true</code> if the schedulable is preemptable;
   *         <code>false</code> otherwise
   */
  boolean isPreemptable();
}
