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

import org.apache.hadoop.yarn.api.records.ReservationDefinition;

/**
 * A Plan represents the central data structure of a reservation system that
 * maintains the "agenda" for the cluster. In particular, it maintains
 * information on how a set of {@link ReservationDefinition} that have been
 * previously accepted will be honored.
 * 
 * {@link ReservationDefinition} submitted by the users through the RM public
 * APIs are passed to appropriate {@link ReservationAgent}s, which in turn will
 * consult the Plan (via the {@link PlanView} interface) and try to determine
 * whether there are sufficient resources available in this Plan to satisfy the
 * temporal and resource constraints of a {@link ReservationDefinition}. If a
 * valid allocation is found the agent will try to store it in the plan (via the
 * {@link PlanEdit} interface). Upon success the system return to the user a
 * positive acknowledgment, and a reservation identifier to be later used to
 * access the reserved resources.
 * 
 * A {@link PlanFollower} will continuously read from the Plan and will
 * affect the instantaneous allocation of resources among jobs running by
 * publishing the "current" slice of the Plan to the underlying scheduler. I.e.,
 * the configuration of queues/weights of the scheduler are modified to reflect
 * the allocations in the Plan.
 * 
 * As this interface have several methods we decompose them into three groups:
 * {@link PlanContext}: containing configuration type information,
 * {@link PlanView} read-only access to the plan state, and {@link PlanEdit}
 * write access to the plan state.
 */
public interface Plan extends PlanContext, PlanView, PlanEdit {

}
