/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.Collection;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.util.Clock;

/**
 * A PlanFollower is a component that runs on a timer, and synchronizes the
 * underlying {@link ResourceScheduler} with the {@link Plan}(s) and viceversa.
 * 
 * While different implementations might operate differently, the key idea is to
 * map the current allocation of resources for each active reservation in the
 * plan(s), to a corresponding notion in the underlying scheduler (e.g., tuning
 * capacity of queues, set pool weights, or tweak application priorities). The
 * goal is to affect the dynamic allocation of resources done by the scheduler
 * so that the jobs obtain access to resources in a way that is consistent with
 * the reservations in the plan. A key conceptual step here is to convert the
 * absolute-valued promises made in the reservations to appropriate relative
 * priorities/queue sizes etc.
 * 
 * Symmetrically the PlanFollower exposes changes in cluster conditions (as
 * tracked by the scheduler) to the plan, e.g., the overall amount of physical
 * resources available. The Plan in turn can react by replanning its allocations
 * if appropriate.
 * 
 * The implementation can assume that is run frequently enough to be able to
 * observe and react to normal operational changes in cluster conditions on the
 * fly (e.g., if cluster resources drop, we can update the relative weights of a
 * queue so that the absolute promises made to the job at reservation time are
 * respected).
 * 
 * However, due to RM restarts and the related downtime, it is advisable for
 * implementations to operate in a stateless way, and be able to synchronize the
 * state of plans/scheduler regardless of how big is the time gap between
 * executions.
 */
public interface PlanFollower extends Runnable {

  /**
   * Init function that configures the PlanFollower, by providing:
   * 
   * @param clock a reference to the system clock.
   * @param sched a reference to the underlying scheduler
   * @param plans references to the plans we should keep synchronized at every
   *          time tick.
   */
  public void init(Clock clock, ResourceScheduler sched, Collection<Plan> plans);

  /**
   * The function performing the actual synchronization operation for a given
   * Plan. This is normally invoked by the run method, but it can be invoked
   * synchronously to avoid race conditions when a user's reservation request
   * start time is imminent.
   * 
   * @param plan the Plan to synchronize
   * @param shouldReplan replan on reduction of plan capacity if true or
   *          proportionally scale down reservations if false
   */
  public void synchronizePlan(Plan plan, boolean shouldReplan);

  /**
   * Setter for the list of plans.
   * 
   * @param plans the collection of Plans we operate on at every time tick.
   */
  public void setPlans(Collection<Plan> plans);

}
