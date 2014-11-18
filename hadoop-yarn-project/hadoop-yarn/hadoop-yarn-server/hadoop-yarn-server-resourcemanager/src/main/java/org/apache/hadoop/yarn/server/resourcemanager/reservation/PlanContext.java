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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * This interface provides read-only access to configuration-type parameter for
 * a plan.
 * 
 */
public interface PlanContext {

  /**
   * Returns the configured "step" or granularity of time of the plan in millis.
   * 
   * @return plan step in millis
   */
  public long getStep();

  /**
   * Return the {@link ReservationAgent} configured for this plan that is
   * responsible for optimally placing various reservation requests
   * 
   * @return the {@link ReservationAgent} configured for this plan
   */
  public ReservationAgent getReservationAgent();

  /**
   * Return an instance of a {@link Planner}, which will be invoked in response
   * to unexpected reduction in the resources of this plan
   * 
   * @return an instance of a {@link Planner}, which will be invoked in response
   *         to unexpected reduction in the resources of this plan
   */
  public Planner getReplanner();

  /**
   * Return the configured {@link SharingPolicy} that governs the sharing of the
   * resources of the plan between its various users
   * 
   * @return the configured {@link SharingPolicy} that governs the sharing of
   *         the resources of the plan between its various users
   */
  public SharingPolicy getSharingPolicy();

  /**
   * Returns the system {@link ResourceCalculator}
   * 
   * @return the system {@link ResourceCalculator}
   */
  public ResourceCalculator getResourceCalculator();

  /**
   * Returns the single smallest {@link Resource} allocation that can be
   * reserved in this plan
   * 
   * @return the single smallest {@link Resource} allocation that can be
   *         reserved in this plan
   */
  public Resource getMinimumAllocation();

  /**
   * Returns the single largest {@link Resource} allocation that can be reserved
   * in this plan
   * 
   * @return the single largest {@link Resource} allocation that can be reserved
   *         in this plan
   */
  public Resource getMaximumAllocation();

  /**
   * Return the name of the queue in the {@link ResourceScheduler} corresponding
   * to this plan
   * 
   * @return the name of the queue in the {@link ResourceScheduler}
   *         corresponding to this plan
   */
  public String getQueueName();

  /**
   * Return the {@link QueueMetrics} for the queue in the
   * {@link ResourceScheduler} corresponding to this plan
   * 
   * @return the {@link QueueMetrics} for the queue in the
   *         {@link ResourceScheduler} corresponding to this plan
   */
  public QueueMetrics getQueueMetrics();

  /**
   * Instructs the {@link PlanFollower} on what to do for applications
   * which are still running when the reservation is expiring (move-to-default
   * vs kill)
   * 
   * @return true if remaining applications have to be killed, false if they
   *         have to migrated
   */
  public boolean getMoveOnExpiry();

}
