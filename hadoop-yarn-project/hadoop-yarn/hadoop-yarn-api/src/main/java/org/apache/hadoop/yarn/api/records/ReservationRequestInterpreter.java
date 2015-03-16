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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Enumeration of various types of dependencies among multiple
 * {@link ReservationRequests} within one {@link ReservationDefinition} (from
 * least constraining to most constraining).
 */
@Public
@Evolving
public enum ReservationRequestInterpreter {
  /**
   * Requires that exactly ONE among the {@link ReservationRequest} submitted as
   * of a {@link ReservationDefinition} is satisfied to satisfy the overall
   * {@link ReservationDefinition}.
   * <p>
   * WHEN TO USE THIS: This is useful when the user have multiple equivalent
   * ways to run an application, and wants to expose to the ReservationAgent
   * such flexibility. For example an application could use one
   * {@literal <32GB,16core>} container for 10min, or 16 {@literal <2GB,1core>}
   * containers for 15min, the ReservationAgent will decide which one of the
   * two it is best for the system to place.
   */
  R_ANY,

  /**
   * Requires that ALL of the {@link ReservationRequest} submitted as part of a
   * {@link ReservationDefinition} are satisfied for the overall
   * {@link ReservationDefinition} to be satisfied. No constraints are imposed
   * on the temporal ordering of the allocation used to satisfy the
   * ResourceRequests.
   * <p>
   * WHEN TO USE THIS: This is useful to capture a scenario in which the user
   * cares for multiple ReservationDefinition to be all accepted, or none. For
   * example, a user might want a reservation R1: with 10 x
   * {@literal <8GB,4core>} for 10min, and a reservation R2:
   * with 2 {@literal <1GB,1core>} for 1h, and only if both are satisfied
   * the workflow run in this reservation succeeds. The key differentiator
   * from ALL and ORDER, ORDER_NO_GAP, is that ALL imposes no restrictions
   * on the relative allocations used to place R1 and R2 above.
   */
  R_ALL,

  /**
   * Requires that ALL of the {@link ReservationRequest} submitted as part of a
   * {@link ReservationDefinition} are satisfied for the overall
   * {@link ReservationDefinition} to be satisfied. Moreover, it imposes a
   * strict temporal ordering on the allocation used to satisfy the
   * {@link ReservationRequest}s. The allocations satisfying the
   * {@link ReservationRequest} in position k must strictly precede the
   * allocations for the {@link ReservationRequest} at position k+1. No
   * constraints are imposed on temporal gaps between subsequent allocations
   * (the last instant of the previous allocation can be an arbitrary long
   * period of time before the first instant of the subsequent allocation).
   * <p>
   * WHEN TO USE THIS: Like ALL this requires all ReservationDefinitions to be
   * placed, but it also imposes a time ordering on the allocations used. This
   * is important if the ReservationDefinition(s) are used to describe a
   * workflow with inherent inter-stage dependencies. For example, a first job
   * runs in a ReservaitonDefinition R1 (10 x {@literal <1GB,1core>}
   * for 20min), and its output is consumed by a second job described by
   * a ReservationDefinition R2 (5 x {@literal <1GB,1core>}) for 50min).
   * R2 allocation cannot overlap R1, as R2 models a job depending on
   * the output of the job modeled by R1.
   */
  R_ORDER,

  /**
   * Requires that ALL of the {@link ReservationRequest} submitted as part of a
   * {@link ReservationDefinition} are satisfied for the overall
   * {@link ReservationDefinition} to be satisfied. Moreover, it imposes a
   * strict temporal ordering on the allocation used to satisfy the
   * {@link ReservationRequest}s. It imposes a strict temporal ordering on the
   * allocation used to satisfy the {@link ReservationRequest}s. The allocations
   * satisfying the {@link ReservationRequest} in position k must strictly
   * precede the allocations for the {@link ReservationRequest} at position k+1.
   * Moreover it imposes a "zero-size gap" between subsequent allocations, i.e.,
   * the last instant in time of the allocations associated with the
   * {@link ReservationRequest} at position k must be exactly preceding the
   * first instant in time of the {@link ReservationRequest} at position k+1.
   * Time ranges are interpreted as [a,b) inclusive left, exclusive right.
   * 
   * WHEN TO USE THIS: This is a stricter version of R_ORDER, which allows no
   * gaps between the allocations that satisfy R1 and R2. The use of this is
   * twofold: 1) prevent long gaps between subsequent stages that produce very
   * large intermediate output (e.g., the output of R1 is too large to be kept
   * around for long before the job running in R2 consumes it, and disposes of
   * it), 2) if the job being modeled has a time-varying resource need, one can
   * combine multiple ResourceDefinition each approximating a portion of the job
   * execution (think of using multiple rectangular bounding boxes to described
   * an arbitrarily shaped area). By asking for no-gaps we guarantee
   * "continuity" of resources given to this job. This still allow for some
   * flexibility, as the entire "train" of allocations can be moved rigidly back
   * or forth within the start-deadline time range (if there is slack).
   * 
   */
  R_ORDER_NO_GAP

}