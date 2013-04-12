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
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;

/**
 * A Schedulable represents an entity that can launch tasks, such as a job
 * or a queue. It provides a common interface so that algorithms such as fair
 * sharing can be applied both within a queue and across queues. There are
 * currently two types of Schedulables: JobSchedulables, which represent a
 * single job, and QueueSchedulables, which allocate among jobs in their queue.
 *
 * Separate sets of Schedulables are used for maps and reduces. Each queue has
 * both a mapSchedulable and a reduceSchedulable, and so does each job.
 *
 * A Schedulable is responsible for three roles:
 * 1) It can launch tasks through assignTask().
 * 2) It provides information about the job/queue to the scheduler, including:
 *    - Demand (maximum number of tasks required)
 *    - Number of currently running tasks
 *    - Minimum share (for queues)
 *    - Job/queue weight (for fair sharing)
 *    - Start time and priority (for FIFO)
 * 3) It can be assigned a fair share, for use with fair scheduling.
 *
 * Schedulable also contains two methods for performing scheduling computations:
 * - updateDemand() is called periodically to compute the demand of the various
 *   jobs and queues, which may be expensive (e.g. jobs must iterate through all
 *   their tasks to count failed tasks, tasks that can be speculated, etc).
 * - redistributeShare() is called after demands are updated and a Schedulable's
 *   fair share has been set by its parent to let it distribute its share among
 *   the other Schedulables within it (e.g. for queues that want to perform fair
 *   sharing among their jobs).
 */
@Private
@Unstable
abstract class Schedulable {
  /** Fair share assigned to this Schedulable */
  private Resource fairShare = Resources.createResource(0);

  /**
   * Name of job/queue, used for debugging as well as for breaking ties in
   * scheduling order deterministically.
   */
  public abstract String getName();

  /**
   * Maximum number of resources required by this Schedulable. This is defined as
   * number of currently utilized resources + number of unlaunched resources (that
   * are either not yet launched or need to be speculated).
   */
  public abstract Resource getDemand();

  /** Get the aggregate amount of resources consumed by the schedulable. */
  public abstract Resource getResourceUsage();

  /** Minimum Resource share assigned to the schedulable. */
  public abstract Resource getMinShare();


  /** Job/queue weight in fair sharing. */
  public abstract double getWeight();

  /** Start time for jobs in FIFO queues; meaningless for QueueSchedulables.*/
  public abstract long getStartTime();

 /** Job priority for jobs in FIFO queues; meaningless for QueueSchedulables. */
  public abstract Priority getPriority();

  /** Refresh the Schedulable's demand and those of its children if any. */
  public abstract void updateDemand();

  /**
   * Assign a container on this node if possible, and return the amount of
   * resources assigned. If {@code reserved} is true, it means a reservation
   * already exists on this node, and the schedulable should fulfill that
   * reservation if possible.
   */
  public abstract Resource assignContainer(FSSchedulerNode node, boolean reserved);

  /** Assign a fair share to this Schedulable. */
  public void setFairShare(Resource fairShare) {
    this.fairShare = fairShare;
  }

  /** Get the fair share assigned to this Schedulable. */
  public Resource getFairShare() {
    return fairShare;
  }

  /** Convenient toString implementation for debugging. */
  @Override
  public String toString() {
    return String.format("[%s, demand=%s, running=%s, share=%s,], w=%.1f]",
        getName(), getDemand(), getResourceUsage(), fairShare, getWeight());
  }
}
