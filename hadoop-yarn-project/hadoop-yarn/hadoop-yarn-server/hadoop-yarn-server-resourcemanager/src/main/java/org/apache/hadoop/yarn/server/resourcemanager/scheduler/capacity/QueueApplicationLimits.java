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

/**
 * The application limit arithmetic of leaf and plan queues. The queues apply
 * it to their live absolute capacities, the configuration resolver to the
 * configured ones, so both compute the limits the same way.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class QueueApplicationLimits {

  private QueueApplicationLimits() {}

  /**
   * The limit a queue without its own maximum-applications is derived from.
   * @param globalPerQueue global-queue-max-application, not set if not
   *                       positive
   * @param maximumSystemApplications maximum-applications of the scheduler
   * @return the base maximum applications
   */
  public static int baseMaximumApplications(int globalPerQueue,
      int maximumSystemApplications) {
    return globalPerQueue > 0
        ? Math.min(globalPerQueue, maximumSystemApplications)
        : maximumSystemApplications;
  }

  /**
   * Whether a queue without its own maximum-applications takes the base
   * limit as it is, instead of scaling it by its absolute capacity.
   * @param globalPerQueue global-queue-max-application, not set if not
   *                       positive
   * @param absoluteResource whether the capacity of the queue is configured
   *                         as an absolute resource
   * @return true if the base limit applies unscaled
   */
  public static boolean usesBaseMaximumApplications(int globalPerQueue,
      boolean absoluteResource) {
    return globalPerQueue > 0 && !absoluteResource;
  }

  /**
   * Scales a limit by an absolute capacity.
   * @param maximumApplications the limit to scale
   * @param absoluteCapacity the absolute capacity, between 0 and 1
   * @return the scaled limit, rounded down
   */
  public static int scaleByAbsoluteCapacity(int maximumApplications,
      float absoluteCapacity) {
    return (int) (maximumApplications * absoluteCapacity);
  }

  /**
   * The maximum applications of one user.
   * @param maximumApplications the maximum applications of the queue
   * @param userLimit minimum-user-limit-percent of the queue
   * @param userLimitFactor user-limit-factor of the queue, -1 for none
   * @param capAtQueueLimit whether the result is at most the queue limit,
   *                        which holds for leaf queues but not for the
   *                        reservations of a plan queue
   * @return the maximum applications per user
   */
  public static int maximumApplicationsPerUser(int maximumApplications,
      float userLimit, float userLimitFactor, boolean capAtQueueLimit) {
    if (userLimitFactor == -1) {
      return maximumApplications;
    }
    int withUserLimits = (int) (maximumApplications
        * (userLimit / 100.0f) * userLimitFactor);
    return capAtQueueLimit ? Math.min(maximumApplications, withUserLimits)
        : withUserLimits;
  }
}
